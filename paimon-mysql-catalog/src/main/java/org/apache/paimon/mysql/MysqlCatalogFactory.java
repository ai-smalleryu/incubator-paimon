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

package org.apache.paimon.mysql;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import static org.apache.paimon.mysql.MyCatalogFactoryOptions.IDENTIFIER;
import static org.apache.paimon.mysql.MyCatalogFactoryOptions.PASSWORD;
import static org.apache.paimon.mysql.MyCatalogFactoryOptions.URL;
import static org.apache.paimon.mysql.MyCatalogFactoryOptions.USERNAME;

/** MysqlCatalogFactory. */
public class MysqlCatalogFactory implements CatalogFactory {

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Catalog create(FileIO fileIO, Path warehouse, CatalogContext context) {
        String url = context.options().get(URL);
        String username = context.options().get(USERNAME);
        String password = context.options().get(PASSWORD);
        return new MysqlCatalog(fileIO, url, username, password, warehouse.toUri().toString());
    }
}
