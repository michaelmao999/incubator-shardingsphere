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

package org.apache.shardingsphere.core.optimize.engine.sharding.query;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.constant.ShardingOperator;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.core.optimize.engine.NewOptimizeEngine;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.*;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExector;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExpression;
import org.apache.shardingsphere.core.strategy.route.value.*;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Query optimize engine.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class NewQueryOptimizeEngine implements NewOptimizeEngine {
    
    private final Group shardingCondition;
    
    private final List<Object> parameters;

    private final SQLFunctionExector sqlFunctionExector;
    
    @Override
    public GroupRouteValue optimize() {
        GroupRouteValue groupRouteValue = shardingAndOptimize(shardingCondition.getExpressions());
        groupRouteValue.optimize();
        boolean isAlwaysFalse = false;
        if (!groupRouteValue.isEmpty()) {
           isAlwaysFalse = checkIfAlwaysFalse(groupRouteValue);
        }
        groupRouteValue.setAlwaysFalse(isAlwaysFalse);
        return groupRouteValue;
    }


    private boolean checkIfAlwaysFalse(GroupRouteValue groupRouteValue) {
        List<RouteValueCondition> conditions = groupRouteValue.getConditionList();
        int len = conditions.size();
        for (int index = 0; index < len; index++) {
            RouteValueCondition condition = conditions.get(index);
            if (condition instanceof  AndValue || condition instanceof OrValue) {
                continue;
            } else if (condition instanceof GroupRouteValue) {
                GroupRouteValue subGroup = (GroupRouteValue)condition;
                if (!subGroup.isEmpty()) {
                    boolean isAlwaysFalse = checkIfAlwaysFalse(subGroup);
                    if (!isAlwaysFalse) {
                        return false;
                    }
                }
            } else if (! (condition instanceof  AlwaysFalseShardingValue)) {
                return false;
            }
        }
        return true;

    }

    private GroupRouteValue shardingAndOptimize(List<SQLCondition> conditions) {
        int len = conditions.size();
        GroupRouteValue group = new GroupRouteValue();
        boolean isLogicExpression = false;
        //0 : Condition, 1 : and   2 : or
//        short andOr = 0;
        for (int index = 0; index < len; index++) {
            SQLCondition condition = conditions.get(index);
            if (isLogicExpression) {
                if (condition instanceof And) {
                    group.add(AndValue.instance);
                } else if (condition instanceof Or) {
                    group.add(OrValue.instance);
                } else {
                    throw new IllegalArgumentException("Wrong SQL expression: " + condition);
                }
                isLogicExpression = false;
            } else {
                if (condition instanceof  Condition) {
                    RouteValue routeValue = shardingOptimize((Condition) condition);
                    if (routeValue instanceof AlwaysFalseShardingValue) {
                        group.removeLastLogicOp();
                        if (group.isEmpty()) {
                            //ignore next logic operator.
                            if (index + 1 < len) {
                                conditions.remove(index + 1);
                                len--;
                                isLogicExpression =false;
                                continue;
                            }
                        }
                    } else {
                        group.add(routeValue);
                    }
                } else if (condition instanceof Group) {
                    GroupRouteValue subGroup = shardingAndOptimize(((Group) condition).getExpressions());
                    if (subGroup.getConditionList().size() == 1) {
                        group.add(subGroup.getConditionList().get(0));
                    } else if (subGroup.isEmpty()) {
                        group.removeLastLogicOp();
                        if (group.isEmpty()) {
                            //ignore next logic operator.
                            if (index + 1 < len) {
                                conditions.remove(index + 1);
                                len--;
                                isLogicExpression =false;
                                continue;
                            }
                        }
                    } else {
                        group.add(subGroup);
                    }
                } else {
                    throw new IllegalArgumentException("Wrong SQL expression: " + condition);
                }
                isLogicExpression = true;
            }
        }
        return group;
    }

    private RouteValue shardingOptimize(Condition condition) {
        Column column = condition.getColumn();
        List<Comparable<?>> listValue = null;
        List<Comparable<?>> conditionValues = condition.getConditionValues(parameters);
        if (!condition.getPositionExpressionMap().isEmpty()) {
            conditionValues = computeConditionValues(conditionValues, condition.getPositionExpressionMap(), parameters);
        }
        if (ShardingOperator.EQUAL == condition.getOperator() || ShardingOperator.IN == condition.getOperator()) {
            listValue = optimize(conditionValues, listValue);
            if (listValue.isEmpty()) {
                return new AlwaysFalseShardingValue();
            } else {
                return new ListRouteValue<>(column.getName(), column.getTableName(), listValue);
            }
        } else if (ShardingOperator.BETWEEN == condition.getOperator()) {
            try {
                Range<Comparable<?>> rangeValue = Range.range(conditionValues.get(0), BoundType.CLOSED, conditionValues.get(1), BoundType.CLOSED);
                return new BetweenRouteValue<>(column.getName(), column.getTableName(), rangeValue);
            } catch (final IllegalArgumentException ex) {
                return new AlwaysFalseShardingValue();
            }
        } else if (ShardingOperator.GT == condition.getOperator()) {
            try {
                Range<? extends Comparable<?>> rangeValue = Range.greaterThan(conditionValues.get(0));
                return new BetweenRouteValue<>(column.getName(), column.getTableName(), rangeValue);
            } catch (final IllegalArgumentException ex) {
                return new AlwaysFalseShardingValue();
            }
        } else if (ShardingOperator.LT == condition.getOperator()) {
            try {
                Range<? extends Comparable<?>> rangeValue = Range.lessThan(conditionValues.get(0));
                return new BetweenRouteValue<>(column.getName(), column.getTableName(), rangeValue);
            } catch (final IllegalArgumentException ex) {
                return new AlwaysFalseShardingValue();
            }
        } else if (ShardingOperator.GT_EQ == condition.getOperator()) {
            try {
                Range<? extends Comparable<?>> rangeValue = Range.atLeast(conditionValues.get(0));
                return new BetweenRouteValue<>(column.getName(), column.getTableName(), rangeValue);
            } catch (final IllegalArgumentException ex) {
                return new AlwaysFalseShardingValue();
            }
        } else if (ShardingOperator.LT_EQ == condition.getOperator()) {
            try {
                Range<? extends Comparable<?>> rangeValue = Range.atMost(conditionValues.get(0));
                return new BetweenRouteValue<>(column.getName(), column.getTableName(), rangeValue);
            } catch (final IllegalArgumentException ex) {
                return new AlwaysFalseShardingValue();
            }
        }
        return new AlwaysFalseShardingValue();
    }


    private List<Comparable<?>> computeConditionValues(List<Comparable<?>> result, Map<Integer, SQLFunctionExpression> positionExpressionMap, final List<Object> parameters) {
        for (Entry<Integer, SQLFunctionExpression> entry : positionExpressionMap.entrySet()) {
            Object parameter = sqlFunctionExector.compute(entry.getValue(), parameters);
            if (!(parameter instanceof Comparable<?>)) {
                throw new ShardingException("Parameter `%s` should extends Comparable for sharding value.", parameter);
            }
            if (entry.getKey() < result.size()) {
                result.add(entry.getKey(), (Comparable<?>) parameter);
            } else {
                result.add((Comparable<?>) parameter);
            }
        }
        return result;
    }


    private List<Comparable<?>> optimize(final List<Comparable<?>> value1, final List<Comparable<?>> value2) {
        if (null == value2) {
            return value1;
        }
        value1.retainAll(value2);
        return value1;
    }
    
}
