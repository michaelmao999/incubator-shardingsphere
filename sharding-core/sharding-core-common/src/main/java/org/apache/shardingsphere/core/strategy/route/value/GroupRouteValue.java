package org.apache.shardingsphere.core.strategy.route.value;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupRouteValue implements RouteValueCondition{
    private List<RouteValueCondition> conditionList = new ArrayList<>();

    private boolean isAlwaysFalse;

    public List<RouteValueCondition> getConditionList() {
        return conditionList;
    }

    public GroupRouteValue add(RouteValueCondition... whereExpressions) {
        for (RouteValueCondition expression : whereExpressions) {
            conditionList.add(expression);
        }
        return this;
    }

    public GroupRouteValue removeLastLogicOp() {
        if (conditionList.isEmpty()) {
            return this;
        }
        RouteValueCondition lastOne = conditionList.get(conditionList.size() - 1);
        if (lastOne instanceof AndValue || lastOne instanceof OrValue) {
            conditionList.remove(conditionList.size() - 1);
        }
        return this;
    }

    public boolean isAlwaysFalse() {
        return isAlwaysFalse;
    }

    public void setAlwaysFalse(boolean alwaysFalse) {
        isAlwaysFalse = alwaysFalse;
    }

    public GroupRouteValue optimize() {
        int len = conditionList.size();
        if (len == 1) {
            RouteValueCondition sqlCondition = conditionList.get(0);
            if (sqlCondition instanceof GroupRouteValue) {
                this.conditionList = ((GroupRouteValue) sqlCondition).conditionList;
            } else if (sqlCondition instanceof AndValue || sqlCondition instanceof OrValue) {
                conditionList.clear();
            }
            return this;
        }
        boolean isFirstExpression = true;
        Map<String, BetweenRouteValue> betweenCountMap = new HashMap<String, BetweenRouteValue>();
        for (int index = 0; index < len; index++) {
            RouteValueCondition sqlCondition = conditionList.get(index);
            if (isFirstExpression) {
                if (sqlCondition instanceof  AndValue || sqlCondition instanceof  OrValue) {
                    conditionList.remove(index);
                    index--;
                    len--;
                    continue;
                } else if (sqlCondition instanceof GroupRouteValue){
                    GroupRouteValue sbuGroup = ((GroupRouteValue) sqlCondition).optimize();
                    if (sbuGroup.size() == 0) {
                        conditionList.remove(index);
                        index--;
                        len--;
                        continue;
                    } else if (conditionList.size() == 1) {
                        conditionList = sbuGroup.conditionList;
                    } else if (sbuGroup.size() == 1) {
                        conditionList.set(index, sbuGroup.getConditionList().get(0));
                    }
                } else if (sqlCondition instanceof BetweenRouteValue) {
                    BetweenRouteValue betweenRouteValue = (BetweenRouteValue) sqlCondition;
                    String key = betweenRouteValue.getTableName() + "." + betweenRouteValue.getColumnName();
                    BetweenRouteValue betweenOp = betweenCountMap.get(key);
                    if (betweenOp == null) {
                        betweenCountMap.put(key, betweenRouteValue);
                    } else {
                        RouteValueCondition previous = conditionList.get(index - 1);
                        if (previous instanceof AndValue) {
                            BetweenRouteValue first = combineTwoSingleBetween(betweenOp, betweenRouteValue);
                            if (first != null) {
                                conditionList.remove(index);
                                conditionList.remove(index - 1);
                                index = index - 2;
                                len = len - 2;
                            }

                        }
                    }
                }
                isFirstExpression = false;
            } else {
                if (sqlCondition instanceof  AndValue || sqlCondition instanceof  OrValue) {
                    isFirstExpression = true;
                } else {
                    throw new IllegalArgumentException("Wrong SQL Expression (there are two condition together)");
                }
            }
        }
        if (conditionList.size() > 0) {
            RouteValueCondition sqlCondition = conditionList.get(conditionList.size() - 1);
            if (sqlCondition instanceof  AndValue || sqlCondition instanceof  OrValue) {
                conditionList.remove(conditionList.size() - 1);
            }
        }
        return this;
    }

    private  BetweenRouteValue combineTwoSingleBetween(BetweenRouteValue first, BetweenRouteValue second) {
        Range range1 = first.getValueRange();
        Range range2 = second.getValueRange();
        if (!range1.hasUpperBound() && !range2.hasLowerBound()) {
            Comparable range1Object = range1.lowerEndpoint();
            BoundType range1Bound = range1.lowerBoundType();
            Comparable range2Object = range2.upperEndpoint();
            BoundType range2Bound = range2.upperBoundType();

            Range newRange = Range.range(range1Object, range1Bound, range2Object, range2Bound);
            first.setValueRange(newRange);
            return first;
        }
        if (!range1.hasLowerBound() && !range2.hasUpperBound()) {
            Comparable range1Object = range1.upperEndpoint();
            BoundType range1Bound = range1.upperBoundType();
            Comparable range2Object = range2.lowerEndpoint();
            BoundType range2Bound = range2.lowerBoundType();
            Range newRange = Range.range( range2Object, range2Bound, range1Object, range1Bound);
            first.setValueRange(newRange);
            return first;
        }
        return null;
    }

    public GroupRouteValue add(GroupRouteValue group) {
        conditionList.add(group);
        return this;
    }

    public int size() {
        return conditionList.size();
    }

    public boolean isEmpty() {
        return conditionList.isEmpty();
    }

}
