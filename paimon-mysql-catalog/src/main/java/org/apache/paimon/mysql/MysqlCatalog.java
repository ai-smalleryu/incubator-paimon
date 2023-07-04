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

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** MysqlCatalog. */
public class MysqlCatalog extends AbstractCatalog {

    private String url;
    private String username;
    private String password;
    private String warehouse;
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    static {
        try {
            Class.forName(MYSQL_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static final String defaultDatabase = "default_database";
    private static final String COMMENT = "comment";

    private boolean sqlExceptionHappened = false;

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public MysqlCatalog(FileIO fileIO, Map<String, String> options) {
        super(fileIO, options);
    }

    public MysqlCatalog(
            FileIO fileIO, String url, String username, String password, String warehouse) {
        super(fileIO);
        this.url = url;
        this.username = username;
        this.password = password;
        this.warehouse = warehouse;
    }

    private Connection connection;

    protected Connection getConnection() {
        try {
            if (connection == null) {
                connection = DriverManager.getConnection(url, username, password);
            }
            if (sqlExceptionHappened) {
                sqlExceptionHappened = false;
                if (!connection.isValid(10)) {
                    connection.close();
                }
                if (connection.isClosed()) {
                    connection = null;
                    return getConnection();
                }
                connection = null;
                return getConnection();
            }

            return connection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String warehouse() {
        return warehouse;
    }

    @Override
    protected TableSchema getDataTableSchema(Identifier identifier) throws TableNotExistException {
        Integer tableId = getTableId(identifier);
        if (tableId == null) {
            throw new TableNotExistException(identifier);
        }
        Path tableLocation = getDataTableLocation(identifier);
        return new SchemaManager(fileIO, tableLocation)
                .latest()
                .orElseThrow(() -> new TableNotExistException(identifier));
    }

    @Override
    public Optional<CatalogLock.Factory> lockFactory() {
        return Optional.empty();
    }

    @Override
    public List<String> listDatabases() {
        List<String> databaseList = new ArrayList<>();
        String querySql = "SELECT database_name FROM metadata_database";
        Connection conn = getConnection();
        try (PreparedStatement ps = conn.prepareStatement(querySql)) {

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String dbName = rs.getString(1);
                databaseList.add(dbName);
            }

            return databaseList;
        } catch (Exception e) {
            throw new RuntimeException("listtable error %s", e);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return getDatabaseId(databaseName) != null;
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(name), "name cannot be null or empty");
        if (databaseExists(name)) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(name);
            }
        } else {
            // 在这里实现创建库的代码
            Connection conn = getConnection();
            // 启动事务
            String insertSql = "insert into metadata_database(database_name) values(?)";

            try (PreparedStatement stat =
                    conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
                conn.setAutoCommit(false);
                stat.setString(1, name);
                stat.executeUpdate();
                conn.commit();
            } catch (SQLException e) {
                sqlExceptionHappened = true;
                throw new DatabaseAlreadyExistException("创建 database 信息失败：", e);
            }
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        if (name.equals(defaultDatabase)) {
            throw new DatabaseNotExistException("默认 database 不可以删除");
        }
        // 1、取出db id，
        Integer id = getDatabaseId(name);
        if (id == null) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(name);
            }
            return;
        }
        Connection conn = getConnection();
        try {
            conn.setAutoCommit(false);
            // 查询是否有表
            List<String> tables = listTables(name);
            if (tables.size() > 0) {
                if (!cascade) {
                    // 有表，不做级联删除。
                    throw new DatabaseNotEmptyException(name);
                }
                // 做级联删除
                for (String table : tables) {
                    try {
                        dropTable(new Identifier(name, table), true);
                    } catch (TableNotExistException t) {

                    }
                }
            }
            // todo: 现在是真实删除，后续设计是否做记录保留。
            String deletePropSql = "delete from metadata_database_property where database_id=?";
            PreparedStatement dStat = conn.prepareStatement(deletePropSql);
            dStat.setInt(1, id);
            dStat.executeUpdate();
            dStat.close();
            String deleteDbSql = "delete from metadata_database where database_id=?";
            dStat = conn.prepareStatement(deleteDbSql);
            dStat.setInt(1, id);
            dStat.executeUpdate();
            dStat.close();
            conn.commit();
        } catch (SQLException e) {
            sqlExceptionHappened = true;
            throw new RuntimeException("删除 database 信息失败：", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        return listTablesNames(databaseName);
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        Integer tableId = getTableId(identifier);
        if (tableId == null) {
            throw new TableNotExistException(identifier);
        }
        Connection connection = getConnection();
        try {
            connection.setAutoCommit(false);
            String deletePropSql = "delete from metadata_table_property " + " where table_id=?";
            PreparedStatement dStat = connection.prepareStatement(deletePropSql);
            dStat.setInt(1, tableId);
            dStat.executeUpdate();
            dStat.close();
            String deleteColSql = "delete from metadata_column " + " where table_id=?";
            dStat = connection.prepareStatement(deleteColSql);
            dStat.setInt(1, tableId);
            dStat.executeUpdate();
            dStat.close();
            String deleteDbSql = "delete from metadata_table " + " where id=?";
            dStat = connection.prepareStatement(deleteDbSql);
            dStat.setInt(1, tableId);
            dStat.executeUpdate();
            dStat.close();
            connection.commit();
        } catch (SQLException e) {
            sqlExceptionHappened = true;
            // 删除失败
        }
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        Integer dbId = getDatabaseId(identifier.getDatabaseName());
        if (null == dbId) {
            throw new DatabaseNotExistException(identifier.getDatabaseName());
        }
        Integer tableId = getTableId(identifier);
        if (tableId != null) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(identifier);
            }
            return;
        }
        copyTableDefaultOptions(schema.options());

        Connection conn = getConnection();
        try {
            conn.setAutoCommit(false);

            String insertSql =
                    "insert into metadata_table(\n"
                            + " table_name,"
                            + " table_type,"
                            + " database_id)"
                            + " values(?,?,?)";
            PreparedStatement iStat =
                    conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
            iStat.setString(1, identifier.getObjectName());
            iStat.setString(2, ObjectType.TABLE);
            iStat.setInt(3, dbId);
            iStat.executeUpdate();
            ResultSet idRs = iStat.getGeneratedKeys();
            if (!idRs.next()) {
                iStat.close();
                throw new TableAlreadyExistException(identifier);
            }
            int id = idRs.getInt(1);
            iStat.close();
            // 插入属性和列

            // table 就可以直接拿properties了。
            List<DataField> fields = schema.fields();
            String propInsertSql =
                    "insert into metadata_table_property(table_id,"
                            + "`key`,`value`) values (?,?,?)";
            PreparedStatement pStat = conn.prepareStatement(propInsertSql);
            for (DataField entry : fields) {
                pStat.setInt(1, id);
                pStat.setString(2, entry.name());
                pStat.setString(3, entry.type().toString());
                pStat.addBatch();
            }
            pStat.executeBatch();
            pStat.close();

            conn.commit();
        } catch (SQLException ex) {
            sqlExceptionHappened = true;
            ex.printStackTrace();
            throw new TableAlreadyExistException(identifier);
        }
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {}

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException {}

    @Override
    public void close() throws Exception {
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            } catch (SQLException e) {
                sqlExceptionHappened = true;
                throw new Exception("Fail to close connection.", e);
            }
        }
    }

    private Integer getDatabaseId(String databaseName) throws RuntimeException {
        String querySql = "select id from metadata_database where database_name=?";
        Connection conn = getConnection();
        try (PreparedStatement ps = conn.prepareStatement(querySql)) {
            ps.setString(1, databaseName);
            ResultSet rs = ps.executeQuery();
            boolean multiDB = false;
            Integer id = null;
            while (rs.next()) {
                if (!multiDB) {
                    id = rs.getInt(1);
                    multiDB = true;
                } else {
                    throw new RuntimeException("存在多个同名database: " + databaseName);
                }
            }
            return id;
        } catch (SQLException e) {
            sqlExceptionHappened = true;
            throw new RuntimeException(String.format("获取 database 信息失败：%s", databaseName), e);
        }
    }

    protected List<String> listTablesNames(String databaseName)
            throws DatabaseNotExistException, RuntimeException {
        Integer databaseId = getDatabaseId(databaseName);
        if (null == databaseId) {
            throw new DatabaseNotExistException(databaseName);
        }

        String querySql =
                "SELECT table_name FROM metadata_table where table_type=? and database_id = ?";
        Connection conn = getConnection();
        try (PreparedStatement ps = conn.prepareStatement(querySql)) {
            ps.setString(1, ObjectType.TABLE);
            ps.setInt(2, databaseId);
            ResultSet rs = ps.executeQuery();

            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                String table = rs.getString(1);
                tables.add(table);
            }
            return tables;
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    private Integer getTableId(Identifier identifier) {
        Integer databaseId = getDatabaseId(identifier.getDatabaseName());
        if (databaseId == null) {
            return null;
        }
        // 获取id
        String getIdSql =
                "select id from metadata_table " + " where table_name=? and database_id=?";
        Connection conn = getConnection();
        try (PreparedStatement gStat = conn.prepareStatement(getIdSql)) {
            gStat.setString(1, identifier.getObjectName());
            gStat.setInt(2, databaseId);
            ResultSet rs = gStat.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            sqlExceptionHappened = true;
            throw new RuntimeException(e);
        }
        return null;
    }

    /** ObejctType. */
    protected static class ObjectType {
        /** 数据库. */
        public static final String DATABASE = "database";

        /** 数据表. */
        public static final String TABLE = "TABLE";
    }
}
