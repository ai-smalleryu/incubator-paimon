/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.mysql.MysqlCatalog;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;

/** Tests for mysqlcatalog. */
public class MysqlCatalogTest {
    @TempDir java.nio.file.Path tempFile;
    protected String warehouse;
    protected FileIO fileIO;
    protected Catalog catalog;
    private static MysqlCatalog mysqlCatalog;

    protected static final Schema DEFAULT_TABLE_SCHEMA =
            new Schema(
                    Lists.newArrayList(
                            new DataField(0, "pk", DataTypes.INT()),
                            new DataField(1, "col1", DataTypes.STRING()),
                            new DataField(2, "col2", DataTypes.STRING())),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Maps.newHashMap(),
                    "");

    @BeforeEach
    public void setUp() throws Exception {
        warehouse = tempFile.toUri().toString();
        //        Options catalogOptions = new Options();
        //        catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse);
        //        CatalogContext catalogContext = CatalogContext.create(new Path(warehouse));
        //        fileIO = FileIO.get(new Path(warehouse),catalogContext);
        mysqlCatalog =
                new MysqlCatalog(
                        null,
                        "jdbc:mysql://localhost:53307/test01?useUnicode=true",
                        "root",
                        "example",
                        "file:///temp/ceshi01");
    }

    @Test
    void testCreate() throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException {
        mysqlCatalog.createTable(
                Identifier.create("test01", "test_table"), DEFAULT_TABLE_SCHEMA, false);
    }

    @Test
    void createDatabase() throws Catalog.DatabaseAlreadyExistException {
        mysqlCatalog.createDatabase("test01", false);
    }
}
