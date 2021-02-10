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

package org.apache.hudi.execution



import java.util
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieSparkSqlWriter.{TableInstantInfo, toProperties}
import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, DataSourceUtils, DataSourceWriteOptions, DefaultSource, HoodieBootstrapPartition, HoodieBootstrapRelation, HoodieSparkSqlWriter, HoodieSparkUtils, HoodieWriterUtils, MergeOnReadSnapshotRelation}
import org.apache.hudi.common.model.{OverwriteNonDefaultsWithLatestAvroPayload, OverwriteWithLatestAvroPayload}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.{HoodieWriteResult, SparkRDDWriteClient}
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.{HoodieException, HoodieIOException}
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncTool}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.payload.AWSDmsAvroPayload
import org.apache.hudi.spark3.internal.HoodieDataSourceInternalTable
import org.apache.log4j.LogManager
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * hudi IUD utils
 */
object HudiSQLUtils {

  val AWSDMSAVROPAYLOAD = classOf[AWSDmsAvroPayload].getName
  val OVERWRITEWITHLATESTVROAYLOAD = classOf[OverwriteWithLatestAvroPayload].getName
  val OVERWRITENONDEFAULTSWITHLATESTAVROPAYLOAD = classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName
  val DEFAULT_DELETE_MARKER = "_hoodie_is_deleted"
  val MERGE_MARKER = "_hoodie_merge_marker"
  private val log = LogManager.getLogger(getClass)

  private val tableConfigCache = CacheBuilder
    .newBuilder()
    .maximumSize(1000)
    .build(new CacheLoader[String, Properties] {
      override def load(k: String): Properties = {
        try {
          new HoodieTableMetaClient(SparkSession.active.sparkContext.hadoopConfiguration, k)
            .getTableConfig.getProperties
        } catch {
          // we catch expected error here
          case e: HoodieIOException =>
            log.error(e.getMessage)
            new Properties()
          case t: Throwable =>
            throw t
        }
      }
    })

  def getPropertiesFromTableConfigCache(path: String): Properties = {
    if (path.isEmpty) {
      throw new HoodieIOException("unexpected empty hoodie table basePath")
    }
    tableConfigCache.get(path)
  }

  private def matchHoodieRelation(relation: BaseRelation): Boolean = relation match {
    case h: HadoopFsRelation if (h.options.get("hoodie.datasource.write.table.type") != null) => true
    case m: MergeOnReadSnapshotRelation => true
    case b: HoodieBootstrapRelation => true
    case _ => false
  }

  def isHudiRelation(table: LogicalPlan): Boolean = {
    table.collect {
      case h: HiveTableRelation if (h.tableMeta.storage.inputFormat.getOrElse("").contains("Hoodie")) => true
      case DataSourceV2Relation(_: HoodieDataSourceInternalTable, _, _, _, _) => true
      case LogicalRelation(r: BaseRelation, _, _, _) if (matchHoodieRelation(r)) => true
    }.nonEmpty
  }

  def buildDefaultParameter(parameters: Map[String, String]): Map[String, String] = {
    HoodieWriterUtils.parametersWithWriteDefaults(parameters)
  }

  def getTablePathFromRelation(table: LogicalPlan): String = {
    getHoodiePropsFromRelation(table).getOrElse("path", "")
  }

  /**
   * build props form relation
   */
  def getHoodiePropsFromRelation(table: LogicalPlan): Map[String, String] = {
    table.collect {
      case h: HiveTableRelation =>
        val table = h.asInstanceOf[HiveTableRelation].tableMeta
        val db = table.identifier.database.getOrElse("default")
        val rawTableName = table.identifier.table
        val tableName = if (rawTableName.endsWith("_ro") || rawTableName.endsWith("_rt")) {
          rawTableName.substring(0, rawTableName.size - 3)
        } else {
          rawTableName
        }
        val savePath = table.storage.locationUri.get.toString
        table.properties ++ Map(HIVE_DATABASE_OPT_KEY -> db,
        "inputformat" -> table.storage.inputFormat.getOrElse(""),
          HIVE_TABLE_OPT_KEY -> tableName, "path" -> savePath)
      case LogicalRelation(r: BaseRelation, _, _, _) =>
        r match {
          case h: HadoopFsRelation => h.options.updated("path", h.options.getOrElse("hoodie.base.path", ""))
          case m: MergeOnReadSnapshotRelation => m.optParams.updated("path", m.optParams.getOrElse("hoodie.base.path", ""))
          case b: HoodieBootstrapRelation => b.optParams.updated("path", b.optParams.getOrElse("hoodie.base.path", ""))
        }
    }.headOption.getOrElse(Map.empty[String, String])
  }

  def getRequredFieldsFromTableConf(tablePath: String): (String, Seq[String]) = {
    val tableConf = tableConfigCache.get(tablePath)
    val recordKeyFields = tableConf.getProperty(RECORDKEY_FIELD_OPT_KEY).split(",").map(_.trim).filter(!_.isEmpty)
    val partitionPathFields = tableConf.getProperty(PARTITIONPATH_FIELD_OPT_KEY).split(",").map(_.trim).filter(!_.isEmpty)
    val preCombineField = tableConf.getProperty(PRECOMBINE_FIELD_OPT_KEY).trim
    (preCombineField, recordKeyFields ++ partitionPathFields)
  }

  def merge(
      df: DataFrame,
      spark: SparkSession,
      tableMeta: Map[String, String],
      trySkipIndex: Boolean = true): Unit = {
    val savePath = tableMeta.getOrElse("path", throw new HoodieException("cannot find table Path, pls check your hoodie table!!!"))
    val payload = HudiSQLUtils.tableConfigCache.get(savePath).getProperty(PAYLOAD_CLASS_OPT_KEY, DEFAULT_PAYLOAD_OPT_VAL)

    val (df2Merge, df2Delete) = payload match {
      case AWSDMSAVROPAYLOAD =>
        (df.drop("Op").withColumnRenamed(MERGE_MARKER, "Op"), null)
      case OVERWRITEWITHLATESTVROAYLOAD | OVERWRITENONDEFAULTSWITHLATESTAVROPAYLOAD =>
        val f: (String) => Boolean = { x => { if (x == "D") true else false } }
        val markDeleteUdf = udf(f)
        (df.drop(DEFAULT_DELETE_MARKER).withColumn(DEFAULT_DELETE_MARKER, markDeleteUdf(col(MERGE_MARKER))).drop(MERGE_MARKER) , null)
      case _ =>
        (df.filter(s"${MERGE_MARKER} != 'D'").drop(MERGE_MARKER),
          df.filter(s"${MERGE_MARKER} = 'D'").drop(MERGE_MARKER))
    }

    if (df2Delete == null) {
      update(df2Merge, buildDefaultParameter(tableMeta), spark, WriteOperationType.UPSERT, trySkipIndex)
    } else {
      df.persist()
      // upsert
      update(df2Merge, buildDefaultParameter(tableMeta), spark, WriteOperationType.UPSERT, trySkipIndex)
      // delete
      update(df2Delete, buildDefaultParameter(tableMeta), spark, WriteOperationType.DELETE, false)
      df.unpersist()
    }
  }

  def update(
      df: DataFrame,
      parameters: Map[String, String],
      spark: SparkSession,
      writeOperationType: WriteOperationType = WriteOperationType.UPSERT,
      trySkipIndex: Boolean = true): Unit = {
    val tablePath = parameters.get("path").get
    val tableConfig = tableConfigCache.get(tablePath)
    tableConfig.put(TABLE_TYPE_OPT_KEY, tableConfig.getProperty("hoodie.table.type"))
    val tblName = tableConfig.get("hoodie.table.name").toString
    val dataWriteOptions = parameters ++ addSetProperties(parameters, spark) ++ tableConfig.asScala

    checkWriteOptions(dataWriteOptions)

    if (df.schema.exists(f => f.name.endsWith(HoodieRecord.FILENAME_METADATA_FIELD)) && trySkipIndex) {
      log.info("try to upsert table skip index")
      val par = dataWriteOptions.getOrElse("par", "200")
      upsertSkipIndex(df,
        dataWriteOptions ++ Map("hoodie.tagging.before.insert" -> "false",
          HoodieWriteConfig.UPSERT_PARALLELISM -> par,
          HoodieWriteConfig.COMBINE_BEFORE_UPSERT_PROP -> "false"),
        spark,
        dataWriteOptions.getOrElse("hoodie.table.name", tblName))
    } else {
      log.info("try to upsert table directly")
      upsertDirectly(df,
        ( dataWriteOptions ++ Map("hoodie.tagging.before.insert" -> "true")).asJava,
        writeOperationType, SaveMode.Append, tablePath)
    }
    doSyncToHive(dataWriteOptions, new Path(tablePath), spark.sparkContext.hadoopConfiguration)
  }

  private def checkWriteOptions(parameters: Map[String, String]): Unit = {
    def checkNeededOption(option: String): Unit = {
      parameters.getOrElse(option, throw new HoodieException(s"cannot find ${option}, pls set it when you create hudi table"))
      checkNeededOption(PRECOMBINE_FIELD_OPT_KEY)
      checkNeededOption(RECORDKEY_FIELD_OPT_KEY)
      checkNeededOption(PARTITIONPATH_FIELD_OPT_KEY)
      checkNeededOption(HIVE_PARTITION_FIELDS_OPT_KEY)
      checkNeededOption(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY)
    }
  }

  /**
   * do merge skipping index tag, which is only used by sql
   * we can use _hoodie_record_key, _hoodie_partition_path, _hoodie_file_name to build tagged hudi rdd directly
   */
  private def upsertSkipIndex(
      df: DataFrame,
      parameters: Map[String, String],
      spark: SparkSession,
      tblName: String): Unit = {
    val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tblName)
    spark.sparkContext.getConf.registerKryoClasses(
      Array(classOf[org.apache.avro.generic.GenericData], classOf[org.apache.avro.Schema]))

    val schema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema, structName, nameSpace)
    spark.sparkContext.getConf.registerAvroSchemas(schema)
    log.info(s"Registered avro schema: ${schema.toString(true)}")

    //
    val typedProperties: TypedProperties = {
      val props = new TypedProperties()
      parameters.foreach(kv => props.setProperty(kv._1, kv._2))
      props
    }
    val keyGenerator = DataSourceUtils.createKeyGenerator(typedProperties)
    // create hoodieRecord directly, insert record is should be considered
    val genericRecords: RDD[GenericRecord] = HoodieSparkUtils.createRdd(df, schema, structName, nameSpace)
    val combineKey = parameters.get(PRECOMBINE_FIELD_OPT_KEY).get.trim
    val hoodieRecords = genericRecords.map { gr =>
      val orderingVal = HoodieAvroUtils
        .getNestedFieldVal(gr, combineKey, false).asInstanceOf[Comparable[_]]

      val (hoodieRecord, canSetLocation) = if (gr.get(RECORDKEY_FIELD_OPT_KEY) == null
        || gr.get(PARTITIONPATH_FIELD_OPT_KEY) == null
        || gr.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD) == null) {
        (DataSourceUtils.createHoodieRecord(gr,
          orderingVal, keyGenerator.getKey(gr), parameters(PAYLOAD_CLASS_OPT_KEY)), false)
      } else {
        (DataSourceUtils.createHoodieRecord(gr,
          orderingVal,
          new HoodieKey(gr.get(RECORDKEY_FIELD_OPT_KEY).toString, gr.get(PARTITIONPATH_FIELD_OPT_KEY).toString),
          parameters(PARTITIONPATH_FIELD_OPT_KEY)), true)
      }

      if (canSetLocation) {
        hoodieRecord.unseal()
        // set location
        hoodieRecord.setCurrentLocation( new HoodieRecordLocation(gr.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString
          , gr.get(HoodieRecord.FILENAME_METADATA_FIELD).toString.split("_")(0)))
        hoodieRecord.seal()
      }
      hoodieRecord
    }.toJavaRDD()

    // create a HoodieWriteClient $ issue the write
    val jsc = new JavaSparkContext(spark.sparkContext)
    val path = parameters.get("path")
    val basePath = new Path(path.get)
    val client = DataSourceUtils.createHoodieClient(jsc,
      schema.toString, path.get, tblName, mapAsJavaMap(parameters - HoodieWriteConfig.HOODIE_AUTO_COMMIT_PROP))
      .asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]

    val instantTime = HoodieActiveTimeline.createNewInstantTime()
    val commitActionType = DataSourceUtils.getCommitActionType(WriteOperationType.UPSERT,
      if (parameters.getOrElse("hoodie.table.type", "COPY_ON_WRITE") == "COPY_ON_WRITE") HoodieTableType.COPY_ON_WRITE else HoodieTableType.MERGE_ON_READ)

    client.startCommitWithTime(instantTime, commitActionType)
    val writeResult = DataSourceUtils.doWriteOperation(client, hoodieRecords, instantTime, WriteOperationType.UPSERT)

    // commit and post commit
    commitAndPerformPostOperations(writeResult,
      parameters,
      client,
      new HoodieTableMetaClient(spark.sparkContext.hadoopConfiguration, path.get).getTableConfig,
      jsc, TableInstantInfo(basePath, instantTime, commitActionType, WriteOperationType.UPSERT))
  }

  private def commitAndPerformPostOperations(
       writeResult: HoodieWriteResult,
       parameters: Map[String, String],
       client: SparkRDDWriteClient[HoodieRecordPayload[Nothing]],
       tableConfig: HoodieTableConfig,
       jsc: JavaSparkContext,
       tableInstantInfo: TableInstantInfo): Unit = {
    val errorCount = writeResult.getWriteStatuses.rdd.filter(ws => ws.hasErrors).count()
    if (errorCount == 0) {
      log.info("No errors. Proceeding to commit the write.")
      val metaMap = parameters.filter(kv => kv._1.startsWith(parameters(COMMIT_METADATA_KEYPREFIX_OPT_KEY)))
      val commitSuccess =
        client.commit(tableInstantInfo.instantTime, writeResult.getWriteStatuses,
          org.apache.hudi.common.util.Option.of(new util.HashMap[String, String](mapAsJavaMap(metaMap))),
          tableInstantInfo.commitActionType, writeResult.getPartitionToReplaceFileIds)
      if (commitSuccess) {
        log.info("Commit" + tableInstantInfo.instantTime + " successful!")
      } else {
        log.info("Commit " + tableInstantInfo.instantTime + " failed!")
      }
    } else {
      log.info(s"${tableInstantInfo.operation} failed with $errorCount errors :")
      if (log.isTraceEnabled) {
        log.trace("Printing out the top 100 errors")
        writeResult.getWriteStatuses.rdd.filter(ws => ws.hasErrors)
          .take(100)
          .foreach(ws => {
            log.trace("Global error :", ws.getGlobalError)
            if (ws.getErrors.size() > 0) {
              ws.getErrors.asScala.foreach(kt => log.trace(s"Error for key: ${kt._1}", kt._2))
            }
          })
      }
    }
  }

  /**
   * do merge directly, just use hoodie dataFrame api
   */
  private def upsertDirectly(df: DataFrame,
       options: java.util.Map[String, String],
       writeOperationType: WriteOperationType = WriteOperationType.UPSERT,
       mode: SaveMode,
       savePath: String): Unit = {
    val par = options.getOrDefault("par", "200")
    df.write.format("hudi")
      .option(HoodieWriteConfig.BULKINSERT_PARALLELISM, par)
      .option(HoodieWriteConfig.INSERT_PARALLELISM, par)
      .option(HoodieWriteConfig.UPSERT_PARALLELISM, par)
      .option(HoodieWriteConfig.DELETE_PARALLELISM, par)
      .options(options)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, writeOperationType.value())
      .mode(mode)
      .save(savePath)
  }

  /**
   * now just sync hudi table info to hive
   * we use hive driver to do sync, maybe it's better to use spark
   */
  private def doSyncToHive(parameters: Map[String, String], basePath: Path, hadoopConf: Configuration): Unit = {
    log.info("Syncing to Hive Start!!!")
    val hiveSyncConfig = buildSyncConfig(basePath, parameters)
    val hiveConf = new HiveConf()
    val fs = basePath.getFileSystem(hadoopConf)
    hiveConf.addResource(fs.getConf)
    new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable()
  }

  private def buildSyncConfig(basePath: Path, parameters: Map[String, String]): HiveSyncConfig = {
    val hiveSyncConfig: HiveSyncConfig = new HiveSyncConfig()
    hiveSyncConfig.basePath = basePath.toString
    hiveSyncConfig.baseFileFormat = parameters(HIVE_BASE_FILE_FORMAT_OPT_KEY);
    hiveSyncConfig.usePreApacheInputFormat =
      parameters.get(HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY).exists(r => r.toBoolean)
    hiveSyncConfig.databaseName = parameters(HIVE_DATABASE_OPT_KEY)
    hiveSyncConfig.tableName = parameters(HIVE_TABLE_OPT_KEY)
    hiveSyncConfig.partitionFields =
      ListBuffer(parameters(HIVE_PARTITION_FIELDS_OPT_KEY).split(",").map(_.trim).filter(!_.isEmpty).toList: _*).asJava
    hiveSyncConfig.partitionValueExtractorClass = parameters(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY)
    hiveSyncConfig.useJdbc = false
    hiveSyncConfig.supportTimestamp = parameters.get(HIVE_SUPPORT_TIMESTAMP).exists(r => r.toBoolean)
    hiveSyncConfig.autoCreateDatabase = parameters.get(HIVE_AUTO_CREATE_DATABASE_OPT_KEY).exists(r => r.toBoolean)
    hiveSyncConfig.decodePartition = parameters.getOrElse(URL_ENCODE_PARTITIONING_OPT_KEY,
      DEFAULT_URL_ENCODE_PARTITIONING_OPT_VAL).toBoolean
    hiveSyncConfig
  }

  /**
   * user can set some properties by sql statement: set xxxx=xx
   * we try to grab some useful property for hudi
   */
  private def addSetProperties(parameters: Map[String, String], spark: SparkSession): Map[String, String] = {
    val par = spark.sessionState.conf.getConfString("hoodie.shuffle.parallelism", "20")
    parameters ++ Map("par" -> par)
  }

  object HoodieV1Relation {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
      case dsv2 @ DataSourceV2Relation(t: HoodieDataSourceInternalTable, _, _, _, options) => Some(fromV2Relation(dsv2, t, options))
      case _ => None
    }

    def fromV2Relation(dsv2: DataSourceV2Relation, table: HoodieDataSourceInternalTable, options: CaseInsensitiveStringMap): LogicalRelation = {
      val datasourceV1 = new DefaultSource
      var properties = table.properties().asScala.toMap ++ options.asScala
      val partitionPath = table.properties().asScala.getOrElse(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "")
        .split(",").map(_ => "*").mkString(java.io.File.separator)

      val tableName = properties.get(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY).getOrElse(table.name())
      if (tableName.endsWith("_rt")) {
        properties = properties ++ Map[String, String](DataSourceReadOptions.QUERY_TYPE_OPT_KEY -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      } else if (tableName.endsWith("_ro")) {
        properties = properties ++ Map[String, String](DataSourceReadOptions.QUERY_TYPE_OPT_KEY -> DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      }

      properties = properties ++ Map("path" -> (properties("path") + java.io.File.separator + partitionPath),
        HIVE_DATABASE_OPT_KEY -> dsv2.identifier.get.namespace().last,
        HIVE_TABLE_OPT_KEY -> dsv2.identifier.get.name(), "basePath" -> properties("path"))

      val relation = datasourceV1.createRelation(SparkSession.active.sqlContext, properties, table.schema())

      LogicalRelation(relation.asInstanceOf[BaseRelation], dsv2.output, None, false)
    }
  }
}
