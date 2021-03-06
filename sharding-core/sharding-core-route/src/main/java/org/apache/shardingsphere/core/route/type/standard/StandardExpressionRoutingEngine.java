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

package org.apache.shardingsphere.core.route.type.standard;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.api.hint.HintManager;
import org.apache.shardingsphere.core.optimize.condition.ShardingCondition;
import org.apache.shardingsphere.core.optimize.result.insert.InsertOptimizeResultUnit;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.route.type.RoutingEngine;
import org.apache.shardingsphere.core.route.type.RoutingResult;
import org.apache.shardingsphere.core.route.type.RoutingUnit;
import org.apache.shardingsphere.core.route.type.TableUnit;
import org.apache.shardingsphere.core.rule.BindingTableRule;
import org.apache.shardingsphere.core.rule.DataNode;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.rule.TableRule;
import org.apache.shardingsphere.core.strategy.route.ShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.hint.HintShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.value.*;

import java.util.*;

/**
 * Standard routing engine for SQL condition expression.
 * 
 * @author michael mao
 */
@RequiredArgsConstructor
public final class StandardExpressionRoutingEngine implements RoutingEngine {
    
    private final SQLStatement sqlStatement;
    
    private final ShardingRule shardingRule;
    
    private final String logicTableName;
    
    private final GroupRouteValue groupRouteValue;
   
    @Override
    public RoutingResult route() {
        return generateRoutingResult(getDataNodes(shardingRule.getTableRule(logicTableName)));
    }
    
    private RoutingResult generateRoutingResult(final Collection<DataNode> routedDataNodes) {
        RoutingResult result = new RoutingResult();
        for (DataNode each : routedDataNodes) {
            RoutingUnit routingUnit = new RoutingUnit(each.getDataSourceName());
            routingUnit.getTableUnits().add(new TableUnit(logicTableName, each.getTableName()));
            result.getRoutingUnits().add(routingUnit);
        }
        return result;
    }
    
    private Collection<DataNode> getDataNodes(final TableRule tableRule) {
        if (shardingRule.isRoutingByHint(tableRule)) {
            return routeByHint(tableRule);
        }
        if (isRoutingByShardingConditions(tableRule)) {
            return routeByShardingConditions(tableRule);
        }
        return routeByMixedConditions(tableRule);
    }
    
    private Collection<DataNode> routeByHint(final TableRule tableRule) {
        return route(tableRule, getDatabaseShardingValuesFromHint(), getTableShardingValuesFromHint());
    }
    
    private boolean isRoutingByShardingConditions(final TableRule tableRule) {
        return !(shardingRule.getDatabaseShardingStrategy(tableRule) instanceof HintShardingStrategy || shardingRule.getTableShardingStrategy(tableRule) instanceof HintShardingStrategy);
    }
    
    private Collection<DataNode> routeByShardingConditions(final TableRule tableRule) {
        return groupRouteValue.isEmpty() ? route(tableRule, Collections.<RouteValue>emptyList(), Collections.<RouteValue>emptyList())
                : routeByShardingConditionsWithCondition(tableRule, groupRouteValue);
    }
    
    private Collection<DataNode> routeByShardingConditionsWithCondition(final TableRule tableRule, final GroupRouteValue group) {
        Collection<DataNode> result = new LinkedList<>();
        List<RouteValue> routeValues = new ArrayList<>();
        Set<String> targetDatabases = new HashSet<>();
        routeDatasourceByCondition(tableRule, group, targetDatabases);
        if (targetDatabases.isEmpty()) {
            Collection<String> availableTargetDatabases = tableRule.getActualDatasourceNames();
            targetDatabases.addAll(availableTargetDatabases);
        }
        short logicOp = 0;
        for (RouteValueCondition condition : group.getConditionList()) {
            if (condition instanceof AndValue) {
                logicOp = 1;
            } else if (condition instanceof OrValue) {
                logicOp = 2;
            } else if (condition instanceof GroupRouteValue){
                Collection<DataNode> subResult = routeByShardingConditionsWithCondition(tableRule, (GroupRouteValue) condition);
                combineResult(result, subResult, logicOp);
                logicOp = 0;
            } else if (condition instanceof RouteValue) {
                routeValues.clear();
                routeValues.add((RouteValue) condition);
                List<RouteValue> tableShardingValues = getShardingValuesFromShardingConditions(shardingRule.getTableShardingStrategy(tableRule).getShardingColumns(), routeValues);
                Collection<DataNode> dataNodes = routeWithDatasource(tableRule, targetDatabases, tableShardingValues);
                //reviseInsertOptimizeResult(routeValues, dataNodes);
                combineResult(result, dataNodes, logicOp);
                logicOp = 0;
            }

        }
        return result;
    }

    private void routeDatasourceByCondition(final TableRule tableRule, final GroupRouteValue group, final Set<String> datasources) {
        Collection<String> columns4DatabaseSharding = shardingRule.getDatabaseShardingStrategy(tableRule).getShardingColumns();
        if (columns4DatabaseSharding.isEmpty()) {
            return;
        }
        List<RouteValue> routeValues = new ArrayList<>();
        int len = group.getConditionList().size();
        for (RouteValueCondition condition : group.getConditionList()) {
            if (condition instanceof GroupRouteValue){
                routeDatasourceByCondition(tableRule, (GroupRouteValue) condition, datasources);
            } else if (condition instanceof RouteValue) {
                routeValues.clear();
                routeValues.add((RouteValue) condition);
                List<RouteValue> databaseShardingValues =  getShardingValuesFromShardingConditions(columns4DatabaseSharding, routeValues);
                if (!databaseShardingValues.isEmpty()) {
                    Collection<String> routedDataSources = routeDataSources(tableRule, databaseShardingValues);
                    if (!routedDataSources.isEmpty()) {
                        datasources.addAll(routedDataSources);
                    }
                }
            }

        }
    }

    private void combineResult(Collection<DataNode> result1, Collection<DataNode> subResult, short logicOp) {
        if (subResult == null || subResult.isEmpty()) {
            //do nothing;
            return;
        }
        if (result1.isEmpty()) {
            result1.addAll(subResult);
            return;
        }
        List<DataNode> newResult = null;
        if (logicOp == 1) {
            newResult = new ArrayList<>();
        }
        Set<DataNode> set1 = new HashSet<>(result1);
        for (DataNode node : subResult) {
            boolean isExist = set1.contains(node);
            //And operator
            if (logicOp == 1) {
                //intersection
                if (isExist) {
                    newResult.add(node);
                }
            } else if (logicOp == 2 || logicOp == 0) {  // or operator , plus
                //union set
                if (!isExist) {
                    result1.add(node);
                }
            }

        }
        if (logicOp == 1) {
            result1.clear();
            result1.addAll(newResult);
        }
    }


    
    private Collection<DataNode> routeByMixedConditions(final TableRule tableRule) {
        return groupRouteValue.isEmpty() ? routeByMixedConditionsWithHint(tableRule) : routeByMixedConditionsWithCondition(tableRule, groupRouteValue);
    }
    
    private Collection<DataNode> routeByMixedConditionsWithCondition(final TableRule tableRule, GroupRouteValue group) {
        Collection<DataNode> result = new LinkedList<>();
        List<RouteValue> routeValues = new ArrayList<>();
        short logicOp = 0;
        for (RouteValueCondition condition : group.getConditionList()) {
            if (condition instanceof AndValue) {
                logicOp = 1;
            } else if (condition instanceof OrValue) {
                logicOp = 2;
            } else if (condition instanceof GroupRouteValue){
                Collection<DataNode> subResult = routeByMixedConditionsWithCondition(tableRule, (GroupRouteValue) condition);
                combineResult(result, subResult, logicOp);
                logicOp = 0;
            } else if (condition instanceof RouteValue) {
                routeValues.clear();
                routeValues.add((RouteValue) condition);
                Collection<DataNode> dataNodes = route(tableRule, getDatabaseShardingValues(tableRule, routeValues), getTableShardingValues(tableRule, routeValues));
                //reviseInsertOptimizeResult(each, dataNodes);
                combineResult(result, dataNodes, logicOp);
                logicOp = 0;
            }
        }
        return result;
    }
    
    private Collection<DataNode> routeByMixedConditionsWithHint(final TableRule tableRule) {
        if (shardingRule.getDatabaseShardingStrategy(tableRule) instanceof HintShardingStrategy) {
            return route(tableRule, getDatabaseShardingValuesFromHint(), Collections.<RouteValue>emptyList());
        }
        return route(tableRule, Collections.<RouteValue>emptyList(), getTableShardingValuesFromHint());
    }
    
//    private List<RouteValue> getDatabaseShardingValues(final TableRule tableRule, final ShardingCondition shardingCondition) {
//        ShardingStrategy dataBaseShardingStrategy = shardingRule.getDatabaseShardingStrategy(tableRule);
//        return isGettingShardingValuesFromHint(dataBaseShardingStrategy)
//                ? getDatabaseShardingValuesFromHint() : getShardingValuesFromShardingConditions(dataBaseShardingStrategy.getShardingColumns(), shardingCondition);
//    }

    private List<RouteValue> getDatabaseShardingValues(final TableRule tableRule, final List<RouteValue> shardingCondition) {
        ShardingStrategy dataBaseShardingStrategy = shardingRule.getDatabaseShardingStrategy(tableRule);
        return isGettingShardingValuesFromHint(dataBaseShardingStrategy)
                ? getDatabaseShardingValuesFromHint() : getShardingValuesFromShardingConditions(dataBaseShardingStrategy.getShardingColumns(), shardingCondition);
    }


//    private List<RouteValue> getTableShardingValues(final TableRule tableRule, final ShardingCondition shardingCondition) {
//        ShardingStrategy tableShardingStrategy = shardingRule.getTableShardingStrategy(tableRule);
//        return isGettingShardingValuesFromHint(tableShardingStrategy)
//                ? getTableShardingValuesFromHint() : getShardingValuesFromShardingConditions(tableShardingStrategy.getShardingColumns(), shardingCondition);
//    }


    private List<RouteValue> getTableShardingValues(final TableRule tableRule, final List<RouteValue> shardingCondition) {
        ShardingStrategy tableShardingStrategy = shardingRule.getTableShardingStrategy(tableRule);
        return isGettingShardingValuesFromHint(tableShardingStrategy)
                ? getTableShardingValuesFromHint() : getShardingValuesFromShardingConditions(tableShardingStrategy.getShardingColumns(), shardingCondition);
    }
    
    private boolean isGettingShardingValuesFromHint(final ShardingStrategy shardingStrategy) {
        return shardingStrategy instanceof HintShardingStrategy;
    }
    
    private List<RouteValue> getDatabaseShardingValuesFromHint() {
        return getRouteValues(HintManager.getDatabaseShardingValues(logicTableName));
    }
    
    private List<RouteValue> getTableShardingValuesFromHint() {
        return getRouteValues(HintManager.getTableShardingValues(logicTableName));
    }
    
    private List<RouteValue> getRouteValues(final Collection<Comparable<?>> shardingValue) {
        return shardingValue.isEmpty() ? Collections.<RouteValue>emptyList() : Collections.<RouteValue>singletonList(new ListRouteValue<>("", logicTableName, shardingValue));
    }

    private List<RouteValue> getShardingValuesFromShardingConditions(final Collection<String> shardingColumns, final List<RouteValue> shardingCondition) {
        List<RouteValue> result = new ArrayList<>(shardingColumns.size());
        for (RouteValue each : shardingCondition) {
            Optional<BindingTableRule> bindingTableRule = shardingRule.findBindingTableRule(logicTableName);
            if ((logicTableName.equals(each.getTableName()) || bindingTableRule.isPresent() && bindingTableRule.get().hasLogicTable(logicTableName))
                    && shardingColumns.contains(each.getColumnName())) {
                result.add(each);
            }
        }
        return result;
    }

//    private List<RouteValue> getShardingValuesFromShardingConditions(final Collection<String> shardingColumns, final ShardingCondition shardingCondition) {
//        List<RouteValue> result = new ArrayList<>(shardingColumns.size());
//        for (RouteValue each : shardingCondition.getShardingValues()) {
//            Optional<BindingTableRule> bindingTableRule = shardingRule.findBindingTableRule(logicTableName);
//            if ((logicTableName.equals(each.getTableName()) || bindingTableRule.isPresent() && bindingTableRule.get().hasLogicTable(logicTableName))
//                    && shardingColumns.contains(each.getColumnName())) {
//                result.add(each);
//            }
//        }
//        return result;
//    }

    private Collection<DataNode> routeWithDatasource(final TableRule tableRule, final Set<String> routedDataSources, final List<RouteValue> tableShardingValues) {
        Collection<DataNode> result = new LinkedList<>();
        for (String each : routedDataSources) {
            result.addAll(routeTables(tableRule, each, tableShardingValues));
        }
        return result;
    }

    private Collection<DataNode> route(final TableRule tableRule, final List<RouteValue> databaseShardingValues, final List<RouteValue> tableShardingValues) {
        Collection<DataNode> result = new LinkedList<>();
        Collection<String> routedDataSources = routeDataSources(tableRule, databaseShardingValues);

        for (String each : routedDataSources) {
            result.addAll(routeTables(tableRule, each, tableShardingValues));
        }
        return removeNonExistNodes(result, tableRule);
    }
    
    private Collection<String> routeDataSources(final TableRule tableRule, final List<RouteValue> databaseShardingValues) {
        Collection<String> availableTargetDatabases = tableRule.getActualDatasourceNames();
        if (databaseShardingValues.isEmpty()) {
            return availableTargetDatabases;
        }
        Collection<String> result = new LinkedHashSet<>(shardingRule.getDatabaseShardingStrategy(tableRule).doSharding(availableTargetDatabases, databaseShardingValues));
        Preconditions.checkState(!result.isEmpty(), "no database route info");
        return result;
    }
    
    private Collection<DataNode> routeTables(final TableRule tableRule, final String routedDataSource, final List<RouteValue> tableShardingValues) {

        Collection<String> availableTargetTables = tableRule.getActualTableNames(routedDataSource);
        Collection<String> routedTables = new LinkedHashSet<>(tableShardingValues.isEmpty() ? availableTargetTables
                : shardingRule.getTableShardingStrategy(tableRule).doSharding(availableTargetTables, tableShardingValues));
        Preconditions.checkState(!routedTables.isEmpty(), "no table route info");
        Collection<DataNode> result = new LinkedList<>();
        for (String each : routedTables) {
            result.add(new DataNode(routedDataSource, each));
        }
        return result;
    }
    
    private Collection<DataNode> removeNonExistNodes(final Collection<DataNode> routedDataNodes, final TableRule tableRule) {
        Collection<DataNode> result = new LinkedList<>();
        Set<DataNode> actualDataNodeSet = new HashSet<>(tableRule.getActualDataNodes());
        for (DataNode each : routedDataNodes) {
            if (actualDataNodeSet.contains(each)) {
                result.add(each);
            }
        }
        return result;
    }
    
//    private void reviseInsertOptimizeResult(final ShardingCondition shardingCondition, final Collection<DataNode> dataNodes) {
//        if (sqlStatement instanceof InsertStatement) {
//            for (InsertOptimizeResultUnit each : optimizeResult.getInsertOptimizeResult().get().getUnits()) {
//                if (isQualifiedInsertOptimizeResult(each, shardingCondition)) {
//                    each.getDataNodes().addAll(dataNodes);
//                }
//            }
//        }
//    }
    
    private boolean isQualifiedInsertOptimizeResult(final InsertOptimizeResultUnit unit, final ShardingCondition shardingCondition) {
        for (RouteValue each : shardingCondition.getShardingValues()) {
            Object columnValue = unit.getColumnValue(each.getColumnName());
            if (!columnValue.equals(((ListRouteValue) each).getValues().iterator().next())) {
                return false;
            }
        }
        return true;
    }
}
