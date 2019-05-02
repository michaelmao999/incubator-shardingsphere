package org.apache.shardingsphere.core.strategy.route.value;

import java.util.ArrayList;
import java.util.List;

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
        RouteValueCondition sqlCondition = conditionList.get(conditionList.size() - 1);
        if (sqlCondition instanceof  AndValue || sqlCondition instanceof  OrValue) {
            conditionList.remove(conditionList.size() - 1);
        }
        return this;
    }

    public GroupRouteValue add(GroupRouteValue group) {
        if (conditionList.isEmpty()) {
            conditionList.addAll(group.getConditionList());
        } else {
            conditionList.add(group);
        }
        return this;
    }

    public int size() {
        return conditionList.size();
    }

    public boolean isEmpty() {
        return conditionList.isEmpty();
    }

}
