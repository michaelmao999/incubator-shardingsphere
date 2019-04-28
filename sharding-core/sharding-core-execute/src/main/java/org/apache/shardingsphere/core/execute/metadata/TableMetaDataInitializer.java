/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.core.execute.metadata;

import com.google.common.base.Optional;
import lombok.SneakyThrows;
import org.apache.shardingsphere.core.execute.ShardingExecuteEngine;
import org.apache.shardingsphere.core.metadata.datasource.DataSourceMetaData;
import org.apache.shardingsphere.core.metadata.datasource.ShardingDataSourceMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.rule.TableRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;

/**
 * Table meta data initializer.
 *
 * @author zhangliang
 */
public final class TableMetaDataInitializer {
    
    private final ShardingDataSourceMetaData shardingDataSourceMetaData;
    
    private final TableMetaDataConnectionManager connectionManager;
    
    private final TableMetaDataLoader tableMetaDataLoader;

    private List<String> tablPrefixList= new ArrayList<String>();
    
    public TableMetaDataInitializer(final ShardingDataSourceMetaData shardingDataSourceMetaData, final ShardingExecuteEngine executeEngine, 
                                    final TableMetaDataConnectionManager connectionManager, final int maxConnectionsSizePerQuery, final boolean isCheckingMetaData) {
        this.shardingDataSourceMetaData = shardingDataSourceMetaData;
        this.connectionManager = connectionManager;
        tableMetaDataLoader = new TableMetaDataLoader(shardingDataSourceMetaData, executeEngine, connectionManager, maxConnectionsSizePerQuery, isCheckingMetaData);
        String tablePrefix = System.getProperty("sharding.tablePrefix");
        if (tablePrefix != null) {
            String[] tables = tablePrefix.split(",");
            for (String table : tables) {
                tablPrefixList.add(table.trim());
            }

        }
    }
    
    /**
     * Load table meta data.
     *
     * @param logicTableName logic table name
     * @param shardingRule sharding rule
     * @return table meta data
     */
    @SneakyThrows
    public TableMetaData load(final String logicTableName, final ShardingRule shardingRule) {
        return tableMetaDataLoader.load(logicTableName, shardingRule);
    }
    
    /**
     * Load all table meta data.
     * 
     * @param shardingRule sharding rule
     * @return all table meta data
     */
    @SneakyThrows
    public Map<String, TableMetaData> load(final ShardingRule shardingRule) {
        Map<String, TableMetaData> result = new HashMap<>();
        result.putAll(loadShardingTables(shardingRule));
        result.putAll(loadDefaultTables(shardingRule));
        return result;
    }
    
    private Map<String, TableMetaData> loadShardingTables(final ShardingRule shardingRule) throws SQLException {
        Map<String, TableMetaData> result = new HashMap<>(shardingRule.getTableRules().size(), 1);
        for (TableRule each : shardingRule.getTableRules()) {
            result.put(each.getLogicTable(), tableMetaDataLoader.load(each.getLogicTable(), shardingRule));
        }
        return result;
    }
    
    private Map<String, TableMetaData> loadDefaultTables(final ShardingRule shardingRule) throws SQLException {
        Map<String, TableMetaData> result = new HashMap<>(shardingRule.getTableRules().size(), 1);
        Optional<String> actualDefaultDataSourceName = shardingRule.findActualDefaultDataSourceName();
        if (actualDefaultDataSourceName.isPresent()) {
            for (String each : getAllTableNames(actualDefaultDataSourceName.get())) {
                result.put(each, tableMetaDataLoader.load(each, shardingRule));
            }
        }
        return result;
    }
    
    private Collection<String> getAllTableNames(final String dataSourceName) throws SQLException {
        Collection<String> result = new LinkedHashSet<>();
        DataSourceMetaData dataSourceMetaData = shardingDataSourceMetaData.getActualDataSourceMetaData(dataSourceName);
        String catalog = null == dataSourceMetaData ? null : dataSourceMetaData.getSchemaName();
        try (Connection connection = connectionManager.getConnection(dataSourceName);
             ResultSet resultSet = connection.getMetaData().getTables(catalog, getSchemaName(connection), null, new String[]{"TABLE"})) {
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                if (isMatchTableName(tableName)) {
                    result.add(tableName);
                }
            }
        }
        return result;
    }
    
    private String getCurrentSchemaName(final Connection connection) throws SQLException {
        try {
            return connection.getSchema();
        } catch (final AbstractMethodError | SQLFeatureNotSupportedException ignore) {
            return null;
        }
    }

    private static String getSchemaName(Connection connection) {
        String schema = null;
        try {
            schema = connection.getSchema();
        } catch (final AbstractMethodError | SQLException ignore ) {
        }
        if (schema == null) {
            try {
                java.sql.DatabaseMetaData dbMetadata = connection.getMetaData();
                String dbName = dbMetadata.getDatabaseProductName();
                if ("ORACLE".equalsIgnoreCase(dbName)) {
                    String userName = dbMetadata.getUserName();
                    ResultSet resultSet1 = dbMetadata.getSchemas();
                    while (resultSet1.next()) {
                        if (userName.equals(resultSet1.getString(1))) {
                            schema = userName;
                            break;
                        }

                    }
                    resultSet1.close();
                }
            } catch (AbstractMethodError | SQLException ignore) {
            }
        }
        return schema;
    }

    private boolean isMatchTableName(String tableName) {
        int pos1 = tableName.indexOf('$');
        if (pos1 >= 0) {
            return false;
        }
        pos1 = tableName.indexOf('/');
        if (pos1 >= 0) {
            return false;
        }
        pos1 = tableName.indexOf('+');
        if (pos1 >= 0) {
            return false;
        }
        boolean isMatch = true;
        if (!tablPrefixList.isEmpty()) {
            isMatch = false;
            int len = tablPrefixList.size();
            tableName = tableName.toLowerCase();
            for (int index = 0; index < len; index++) {
                if (tableName.startsWith(tablPrefixList.get(index))) {
                    isMatch = true;
                    break;
                }
            }
        }
        return isMatch;

    }



}
