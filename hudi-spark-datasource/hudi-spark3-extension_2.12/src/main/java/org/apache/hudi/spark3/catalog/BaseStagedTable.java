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

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Set;

/**
 * The BaseEffortStagedTable.
 *
 * @since 2021/1/13
 */
public class BaseStagedTable implements SupportsWrite, StagedTable {

  private Identifier identifier;
  private Table table;
  private TableCatalog catalog;

  public BaseStagedTable(Identifier identifier,
      Table table,
      TableCatalog catalog) {

    this.identifier = identifier;
    this.table = table;
    this.catalog = catalog;
  }

  @Override
  public void commitStagedChanges() {
  }

  @Override
  public void abortStagedChanges() {
    catalog.dropTable(identifier);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
    if (table instanceof SupportsWrite) {
      return ((SupportsWrite) table).newWriteBuilder(logicalWriteInfo);
    }
    throw new RuntimeException("Table implementation does not support writes: " + identifier.name());
  }

  @Override
  public String name() {
    return identifier.name();
  }

  @Override
  public StructType schema() {
    return table.schema();
  }

  @Override
  public Transform[] partitioning() {
    return table.partitioning();
  }

  @Override
  public Map<String, String> properties() {
    return table.properties();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return table.capabilities();
  }
}
