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

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hoodie's Implementation of {@link SupportsWrite}. This is used in data source "hudi.spark3.internal" implementation for bulk insert.
 */
public class HoodieDataSourceInternalTable implements SupportsWrite {

  private final String instantTime;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final SparkSession jss;
  private final Configuration hadoopConfiguration;
  private Map<String, String> properties;

  public HoodieDataSourceInternalTable(String instantTime, HoodieWriteConfig config,
      StructType schema, SparkSession jss, Configuration hadoopConfiguration) {
    this.instantTime = instantTime;
    this.writeConfig = config;
    this.structType = schema;
    this.jss = jss;
    this.hadoopConfiguration = hadoopConfiguration;
    this.properties = config.getProps().entrySet().stream()
        .collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue())));
  }

  @Override
  public String name() {
    String name = writeConfig.getTableName();
    return StringUtils.isNullOrEmpty(name) ? "" : name;
  }

  @Override
  public Transform[] partitioning() {
    List<List<String>> partitionKeys = Arrays.stream(this.properties
            .getOrDefault("hoodie.datasource.hive_sync.partition_fields", "")
            .split(",")).map(String::trim)
            .filter(p -> !p.isEmpty()).map(p -> Arrays.asList(new String[] {p})).collect(Collectors.toList());
    Transform[] result = new Transform[partitionKeys.size()];
    for (int i = 0; i < partitionKeys.size(); i++) {
      result[i] = new IdentityTransform(new FieldReference(JavaConverters.asScalaIterator(partitionKeys.get(i).iterator()).toSeq()));
    }
    return result;
  }

  @Override
  public Map<String, String> properties() {
    return this.properties;
  }

  @Override
  public StructType schema() {
    return structType;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return new HashSet<TableCapability>() {{
        add(TableCapability.BATCH_WRITE);
        add(TableCapability.TRUNCATE);
        add(TableCapability.BATCH_READ);
        add(TableCapability.V1_BATCH_WRITE);
        add(TableCapability.OVERWRITE_BY_FILTER);
      }};
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
    return new HoodieDataSourceInternalBatchWriteBuilder(instantTime, writeConfig, structType, jss,
        hadoopConfiguration, properties);
  }
}
