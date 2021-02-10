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

package org.apache.hudi.spark3.internal;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.V1WriteBuilder;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieWriteConfig.BULKINSERT_PARALLELISM;
import static org.apache.hudi.config.HoodieWriteConfig.DELETE_PARALLELISM;
import static org.apache.hudi.config.HoodieWriteConfig.INSERT_PARALLELISM;
import static org.apache.hudi.config.HoodieWriteConfig.UPSERT_PARALLELISM;

/**
 * Implementation of {@link WriteBuilder} for datasource "hudi.spark3.internal" to be used in datasource implementation
 * of bulk insert.
 */
public class HoodieDataSourceInternalBatchWriteBuilder implements WriteBuilder, V1WriteBuilder, SupportsOverwrite {

  private final String instantTime;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final SparkSession jss;
  private final Configuration hadoopConfiguration;
  private final Map<String, String> properties;
  private WriteOperationType operationType = WriteOperationType.INSERT;

  public HoodieDataSourceInternalBatchWriteBuilder(String instantTime, HoodieWriteConfig writeConfig, StructType structType,
      SparkSession jss, Configuration hadoopConfiguration, Map<String, String> properties) {
    this.instantTime = instantTime;
    this.writeConfig = writeConfig;
    this.structType = structType;
    this.jss = jss;
    this.hadoopConfiguration = hadoopConfiguration;
    this.properties = properties;
  }

  public HoodieDataSourceInternalBatchWriteBuilder(String instantTime, HoodieWriteConfig writeConfig, StructType structType,
      SparkSession jss, Configuration hadoopConfiguration) {
    this(instantTime, writeConfig, structType, jss, hadoopConfiguration, Collections.emptyMap());
  }

  @Override
  public BatchWrite buildForBatch() {
    return new HoodieDataSourceInternalBatchWrite(instantTime, writeConfig, structType, jss,
        hadoopConfiguration);
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    String[] pk = filters[0].references();
    List<String> partitionKeys = Arrays.stream(this.properties
        .getOrDefault("hoodie.datasource.hive_sync.partition_fields", "")
        .split(",")).map(String::trim).collect(Collectors.toList());
    if (pk.length == 0 || !partitionKeys.get(0).equalsIgnoreCase(pk[0])) {
      operationType = WriteOperationType.INSERT_OVERWRITE_TABLE;
    } else {
      operationType = WriteOperationType.INSERT_OVERWRITE;
    }
    return this;
  }

  @Override
  public InsertableRelation buildForV1Write() {
    return new InsertableRelation() {
      @Override
      public void insert(Dataset<Row> data, boolean overwrite) {
        String tablePath = properties.get("path");
        String mode = "append";
        if (overwrite) {
          mode = "overwrite";
        }
        String par = data.sparkSession().sessionState().conf().getConfString("hoodie.shuffle.parallelism", "20");
        data.drop(JavaConverters.asScalaBuffer(HoodieRecord.HOODIE_META_COLUMNS).toList()).write()
            .format("hudi").options(properties).option(BULKINSERT_PARALLELISM, par)
            .option(DELETE_PARALLELISM, par).option(UPSERT_PARALLELISM, par).option(INSERT_PARALLELISM, par)
            .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), operationType.value()).mode(mode).save(tablePath);
      }
    };
  }
}
