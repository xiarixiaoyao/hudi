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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.V1WriteBuilder;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import scala.Option;

/**
 * The StageHoodieTableV2.
 *
 * @since 2021/1/13
 */
public class StageHoodieTableV2 implements SupportsWrite, StagedTable {

    private Identifier identifier;
    private HoodieCatalog catalog;
    private StructType schema;
    private Transform[] paritions;
    private Map<String, String> properties;
    private TableCreationModes modes;
    Option<Dataset> sourceQuery;

    public StageHoodieTableV2(Identifier identifier,
                              HoodieCatalog catalog,
                              StructType schema,
                              Transform[] paritions,
                              Map<String, String> properties,
                              TableCreationModes modes) {

        this.identifier = identifier;
        this.catalog = catalog;
        this.schema = schema;
        this.paritions = paritions;
        this.properties = properties;
        this.modes = modes;
    }

    @Override
    public void commitStagedChanges() {
        try {
            catalog.createHoodieTable(identifier, schema, paritions, properties, sourceQuery, modes);
        } catch (IOException | NoSuchTableException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abortStagedChanges() {

    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
        if (this.properties == null) {
            this.properties = new HashMap<String, String>();
        }
        this.properties.putAll(logicalWriteInfo.options().asCaseSensitiveMap());
        return new HoodieV1WriteBuilder();
    }

    @Override
    public String name() {
        return identifier.name();
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        Set<TableCapability> tableCapabilities = new HashSet<>();
        tableCapabilities.add(TableCapability.V1_BATCH_WRITE);
        return tableCapabilities;
    }

    public class HoodieV1WriteBuilder implements WriteBuilder, V1WriteBuilder {

        @Override
        public InsertableRelation buildForV1Write() {
            return new InsertableRelation() {
                @Override
                public void insert(Dataset<Row> data, boolean overwrite) {
                    sourceQuery = Option.apply(data);
                }
            };
        }
    }
}
