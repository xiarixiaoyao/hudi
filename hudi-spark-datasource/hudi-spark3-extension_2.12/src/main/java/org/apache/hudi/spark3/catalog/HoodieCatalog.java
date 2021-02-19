/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.spark3.catalog;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.hive.NonPartitionedExtractor;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;
import org.apache.hudi.internal.DataSourceInternalWriterHelper;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.spark3.internal.HoodieDataSourceInternalTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * The HoodieCatalog.
 *
 * @since 2021/1/3
 */
public class HoodieCatalog extends DelegatingCatalogExtension implements StagingTableCatalog {

  private static final Logger LOG = LogManager.getLogger(HoodieCatalog.class);

  private SparkSession sparkSession;

  public HoodieCatalog(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  public HoodieCatalog() {
    this(SparkSession.active());
  }

  public Table createHoodieTable(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties, Option<Dataset> sourceQuery, TableCreationModes createMode)
      throws IOException, NoSuchTableException {

    String partitionStr = String.join(",", convertTransforms(partitions));
    properties = updatePartitionProperties(properties, partitionStr);

    checkRequiredTableProperties(properties);

    Option<String> dbName = Option.empty();
    if (ident.namespace() != null && ident.namespace().length > 0) {
      dbName = Option.apply(ident.namespace()[ident.namespace().length - 1]);
    }
    Option<URI> locUriOpt = Option.empty();
    if (properties.containsKey("location")) {
      locUriOpt = Option.apply(DataSourceUtils.stringToURI(properties.get("location")));
    }
    TableIdentifier tableIdentifier = new TableIdentifier(ident.name(), dbName);
    String locStr = locUriOpt.getOrElse(() -> sparkSession.sessionState().catalog().defaultTablePath(tableIdentifier)).toString();
    Path tablePath = new Path(locStr);

    Configuration conf = sparkSession.sparkContext().hadoopConfiguration();
    FileSystem fs = FSUtils.getFs(locStr, conf);
    if (fs.exists(tablePath)) {
      FileStatus fileStatus = fs.getFileStatus(tablePath);
      cleanExtraData(createMode, tablePath, conf, fs, fileStatus);
    }

    HoodieTableConfig hoodieTableConfig = buildHoodieTableConfig(properties, tableIdentifier, locStr);

    properties = hoodieTableConfig.getProps();

    HoodieTableMetaClient hoodieTableMetaClient = HoodieTableMetaClient.initTableAndGetMetaClient(conf, locStr, hoodieTableConfig.getProperties());

    initFirstCommit(ident, schema, properties, sourceQuery, locStr, hoodieTableMetaClient);

    saveSourceDF(sourceQuery, properties);

    syncHive(hoodieTableConfig, tableIdentifier, locStr, conf, fs);

    return loadTable(ident);
  }

  @NotNull
  private Map<String, String> updatePartitionProperties(Map<String, String> properties, String partitionStr) {
    properties = properties.entrySet().stream()
        .collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue())));
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, partitionStr);
    properties.put(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), partitionStr);
    if (StringUtils.isNullOrEmpty(partitionStr)) {
      properties.put(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(), NonPartitionedExtractor.class.getCanonicalName());
    }
    return properties;
  }

  private void checkRequiredTableProperties(Map<String, String> properties) {
    String[] optionCheckList = new String[]{
        DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY,
        DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY()
    };
    List<String> optionCheckResult = new ArrayList<>();
    Arrays.stream(optionCheckList).forEach(item -> {
      if (!properties.containsKey(item)) {
        optionCheckResult.add(item);
      }
    });
    if (optionCheckResult.size() > 0) {
      throw new HoodieException(String.join(",", optionCheckResult) + " could not be empty");
    }
  }

  private HoodieTableConfig buildHoodieTableConfig(Map<String, String> properties, TableIdentifier tableIdentifier, String basePath) {

    properties.put(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, tableIdentifier.table());

    HoodieTableConfig hoodieTableConfig = new HoodieTableConfig(properties);

    HoodieTableType hoodieTableType = hoodieTableConfig.getTableType();
    String archiveFolder = hoodieTableConfig.getArchivelogFolder();
    String payloadClass = hoodieTableConfig.getPayloadClass();

    org.apache.hudi.common.util.Option<TimelineLayoutVersion> layoutVersion = hoodieTableConfig.getTimelineLayoutVersion();
    HoodieFileFormat baseFileFormat = hoodieTableConfig.getBaseFileFormat();

    hoodieTableConfig.getProperties().setProperty(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, hoodieTableType.name());
    hoodieTableConfig.getProperties().setProperty(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), hoodieTableType.name());

    hoodieTableConfig.getProperties().setProperty(HoodieTableConfig.HOODIE_ARCHIVELOG_FOLDER_PROP_NAME, archiveFolder);

    hoodieTableConfig.getProperties().setProperty(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP_NAME, payloadClass);
    hoodieTableConfig.getProperties().setProperty(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY(), payloadClass);

    hoodieTableConfig.getProperties().setProperty(HoodieTableConfig.HOODIE_TIMELINE_LAYOUT_VERSION, String.valueOf(layoutVersion.orElse(new TimelineLayoutVersion(0)).getVersion()));

    hoodieTableConfig.getProperties().setProperty(HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP_NAME, baseFileFormat.name());

    hoodieTableConfig.getProperties().setProperty(HoodieWriteConfig.BASE_PATH_PROP, basePath);
    hoodieTableConfig.getProperties().setProperty("path", basePath);

    if (!hoodieTableConfig.getProperties().containsKey(DataSourceWriteOptions.ENABLE_ROW_WRITER_OPT_KEY())) {
      hoodieTableConfig.getProperties().setProperty(DataSourceWriteOptions.ENABLE_ROW_WRITER_OPT_KEY(), "true");
    }

    if (!hoodieTableConfig.getProperties().containsKey(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY())) {
      hoodieTableConfig.getProperties().setProperty(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(),
          DataSourceWriteOptions.DEFAULT_KEYGENERATOR_CLASS_OPT_VAL());
    }

    return hoodieTableConfig;
  }

  private void cleanExtraData(TableCreationModes createMode, Path tablePath, Configuration conf, FileSystem fs, FileStatus fileStatus) throws IOException {
    // when create external table, if the path had exist:
    //  1) truncate the path when createMode=replace or createMode=createOrReplace
    //  2）rebuild hoodie.properties and sync hive metadata when createMode=create
    if (fileStatus.isDirectory() && (createMode == TableCreationModes.Replace || createMode == TableCreationModes.CreateOrReplace)) {
      FSUtils.moveToTrash(fs, tablePath, conf, false);
    }
    if (fileStatus.isDirectory() && createMode == TableCreationModes.Create) {
      Path metaPathDir = new Path(tablePath, HoodieTableMetaClient.METAFOLDER_NAME);
      Path propertyPath = new Path(metaPathDir, HoodieTableConfig.HOODIE_PROPERTIES_FILE);
      fs.delete(propertyPath, true);
    }
  }

  private void initFirstCommit(Identifier ident, StructType schema, Map<String, String> properties, Option<Dataset> sourceQuery, String locStr, HoodieTableMetaClient hoodieTableMetaClient) {
    if (sourceQuery.isEmpty() && hoodieTableMetaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().empty()) {

      Tuple2<String, String> structTypeAndNameSpace = AvroConversionUtils.getAvroRecordNameAndNamespace(ident.name());
      String schemaStr = AvroConversionUtils.convertStructTypeToAvroSchema(schema, structTypeAndNameSpace._1, structTypeAndNameSpace._2).toString();
      JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
      try (SparkRDDWriteClient client = DataSourceUtils.createHoodieClient(jsc, schemaStr, locStr, ident.name(), properties)) {
        String instantTime = "000";
        client.startCommitWithTime(instantTime);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(new ArrayList(), 1);
        JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, instantTime);
        client.commit(instantTime, result);
      }
    }
  }

  private void saveSourceDF(Option<Dataset> sourceQuery, Map<String, String> properties) {
    sourceQuery.map(df -> {
      String instantTime = HoodieActiveTimeline.createNewInstantTime();

      df.write().format("org.apache.hudi")
          .options(properties)
          .option(DataSourceInternalWriterHelper.INSTANT_TIME_OPT_KEY, instantTime)
          .option(HoodieWriteConfig.BULKINSERT_INPUT_DATA_SCHEMA_DDL, df.schema().toDDL())
          .mode(SaveMode.Append)
          .save();
      return df;
    });
  }

  private List<String> convertTransforms(Transform[] partitions) {
    List<String> identityCols = new ArrayList<>();
    Arrays.stream(partitions).forEach(partition -> {
      if (partition instanceof IdentityTransform) {
        IdentityTransform identityTransform = (IdentityTransform) partition;
        identityCols.addAll(JavaConverters.asJavaCollection(((FieldReference) identityTransform.reference()).parts()));
      }
    });
    return identityCols;
  }

  public void syncHive(HoodieTableConfig hoodieTableConfig, TableIdentifier tableIdentifier, String tableBasePath, Configuration conf, FileSystem fs) {

    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.basePath = tableBasePath;
    hiveSyncConfig.usePreApacheInputFormat = Boolean.valueOf((String) hoodieTableConfig.getProperties()
        .getOrDefault(DataSourceWriteOptions.HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY(),
            DataSourceWriteOptions.DEFAULT_USE_PRE_APACHE_INPUT_FORMAT_OPT_VAL()));
    hiveSyncConfig.databaseName = tableIdentifier.database().getOrElse(() -> DataSourceWriteOptions.DEFAULT_HIVE_DATABASE_OPT_VAL());
    hiveSyncConfig.tableName = tableIdentifier.table();
    hiveSyncConfig.baseFileFormat = hoodieTableConfig.getBaseFileFormat().name();
    hiveSyncConfig.partitionFields = Arrays.asList(hoodieTableConfig.getProperties().getProperty(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY()).split(","));
    hiveSyncConfig.partitionValueExtractorClass = hoodieTableConfig.getProperties()
        .getProperty(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(), SlashEncodedDayPartitionValueExtractor.class.getName());
    hiveSyncConfig.useJdbc = false;
    hiveSyncConfig.autoCreateDatabase = Boolean
        .valueOf(hoodieTableConfig.getProperties().getProperty(DataSourceWriteOptions.HIVE_AUTO_CREATE_DATABASE_OPT_KEY(), DataSourceWriteOptions.DEFAULT_HIVE_AUTO_CREATE_DATABASE_OPT_KEY()));
    hiveSyncConfig.skipROSuffix = Boolean
        .valueOf(hoodieTableConfig.getProperties().getProperty(DataSourceWriteOptions.HIVE_SKIP_RO_SUFFIX(), DataSourceWriteOptions.DEFAULT_HIVE_SKIP_RO_SUFFIX_VAL()));
    hiveSyncConfig.supportTimestamp = Boolean
        .valueOf(hoodieTableConfig.getProperties().getProperty(DataSourceWriteOptions.HIVE_SUPPORT_TIMESTAMP(), DataSourceWriteOptions.DEFAULT_HIVE_SUPPORT_TIMESTAMP()));

    LOG.info("Syncing target hoodie table with hive table(" + hiveSyncConfig.tableName + "), basePath :" + tableBasePath);
    HiveConf hiveConf = new HiveConf(conf, HiveConf.class);
    LOG.debug("Hive Conf => " + hiveConf.getAllProperties().toString());
    LOG.debug("Hive Sync Conf => " + hiveSyncConfig.toString());
    new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable();
  }

  private scala.collection.Map filterProperties(Map<String, String> properties) {
    List<String> notV1Properties = new ArrayList<>();
    notV1Properties.add(TableCatalog.PROP_LOCATION);
    notV1Properties.add(TableCatalog.PROP_PROVIDER);
    notV1Properties.add(TableCatalog.PROP_COMMENT);
    notV1Properties.add(TableCatalog.PROP_OWNER);
    notV1Properties.add("path");
    scala.collection.Map tableProperties = JavaConverters.mapAsScalaMap(properties).filterKeys(key -> !notV1Properties.contains(key));
    return tableProperties;
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    Table table = null;
    try {
      table = super.loadTable(ident);
    } catch (NoSuchTableException e) {
      Identifier rtTable = new HoodieIdentifier(ident.namespace(), ident.name() + "_rt");
      try {
        table = super.loadTable(rtTable);
      } catch (NoSuchTableException nse) {
        Identifier roTable = new HoodieIdentifier(ident.namespace(), ident.name() + "_ro");
        table = super.loadTable(roTable);
      }
    }

    if (table instanceof V1Table && DataSourceUtils.isHoodieTable(((V1Table) table).catalogTable())) {
      String path = getHoodieTableBasePath((V1Table) table);

      String metaPath = new Path(path, HoodieTableMetaClient.METAFOLDER_NAME).toString();
      Configuration conf = sparkSession.sparkContext().hadoopConfiguration();
      FileSystem fs = FSUtils.getFs(metaPath, conf);
      HoodieTableConfig metaConfig = new HoodieTableConfig(fs, metaPath, null);

      metaConfig.getProperties().putAll(table.properties());

      metaConfig.getProperties().put(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), table.name());

      Map<String, String> prop = metaConfig.getProps();
      prop.put(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), metaConfig.getTableType().name());
      prop.put("path", path);
      prop.put(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY(), metaConfig.getPayloadClass());

      String instantTime = (String) metaConfig.getProperties().getOrDefault(DataSourceInternalWriterHelper.INSTANT_TIME_OPT_KEY, "");

      HoodieWriteConfig config = DataSourceUtils.createHoodieConfig(null, path, table.name(), metaConfig.getProps());
      return new HoodieDataSourceInternalTable(instantTime, config, table.schema(), sparkSession, sparkSession.sparkContext().hadoopConfiguration());
    } else {
      return table;
    }
  }

  private String getHoodieTableBasePath(V1Table table) {
    if (table.properties().containsKey("path")) {
      return table.properties().get("path");
    } else {
      return table.catalogTable().storage().locationUri().get().toString();
    }
  }

  @Override
  public boolean tableExists(Identifier ident) {
    try {
      return this.loadTable(ident) != null;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public Table createTable(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    try {
      if (DataSourceUtils.isHoodieTable(getProvider(properties))) {
        return createHoodieTable(ident, schema, partitions, properties, Option.empty(), TableCreationModes.Create);
      } else {
        return super.createTable(ident, schema, partitions, properties);
      }
    } catch (NoSuchTableException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    String tablePath = "";
    try {
      Table table = loadTable(ident);
      if (table instanceof HoodieDataSourceInternalTable) {
        HoodieDataSourceInternalTable hoodieTable = (HoodieDataSourceInternalTable) table;
        tablePath = hoodieTable.properties().get(HoodieWriteConfig.BASE_PATH_PROP);
        Configuration conf = sparkSession.sparkContext().hadoopConfiguration();
        FileSystem fs = FSUtils.getFs(tablePath, conf);
        FSUtils.moveToTrash(fs, new Path(tablePath), conf, false);
      } else {
        return super.dropTable(ident);
      }
    } catch (NoSuchTableException e) {
      LOG.trace("Table [" + ident.toString() + "] not exists when drop table, do nothing");
    } catch (IOException e) {
      LOG.warn("Delete path [" + tablePath + "] fail when drop table table [" + ident.toString() + "], do nothing");
    }
    return super.dropTable(ident);
  }

  private String getProvider(Map<String, String> properties) {
    return Option.apply(properties.get("provider")).getOrElse(() -> (String) sparkSession.sessionState().conf().getConf(SQLConf.DEFAULT_DATA_SOURCE_NAME()));
  }

  @Override
  public StagedTable stageCreate(Identifier identifier, StructType schema, Transform[] partitions, Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    if (DataSourceUtils.isHoodieTable(getProvider(properties))) {
      return new StageHoodieTableV2(identifier, this, schema, partitions, properties, TableCreationModes.Create);
    } else {
      return new BaseStagedTable(identifier, super.createTable(identifier, schema, partitions, properties), this);
    }
  }

  @Override
  public StagedTable stageReplace(Identifier identifier, StructType schema, Transform[] partitions, Map<String, String> properties) throws NoSuchNamespaceException, NoSuchTableException {
    if (DataSourceUtils.isHoodieTable(getProvider(properties))) {
      return new StageHoodieTableV2(identifier, this, schema, partitions, properties, TableCreationModes.CreateOrReplace);
    } else {
      super.dropTable(identifier);
      try {
        return new BaseStagedTable(identifier, super.createTable(identifier, schema, partitions, properties), this);
      } catch (TableAlreadyExistsException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public StagedTable stageCreateOrReplace(Identifier identifier, StructType schema, Transform[] partitions, Map<String, String> properties) throws NoSuchNamespaceException {
    if (DataSourceUtils.isHoodieTable(getProvider(properties))) {
      return new StageHoodieTableV2(identifier, this, schema, partitions, properties, TableCreationModes.Replace);
    } else {
      try {
        super.dropTable(identifier);
        return new BaseStagedTable(identifier, super.createTable(identifier, schema, partitions, properties), this);
      } catch (TableAlreadyExistsException e) {
        throw new RuntimeException(e);
      }
    }
  }

}