package org.apache.shardingsphere.core.strategy.route.value;

public class AndValue implements  RouteValueCondition{
    public  static AndValue instance = new AndValue();

    public String toString() {
        return " and ";
    }
}
