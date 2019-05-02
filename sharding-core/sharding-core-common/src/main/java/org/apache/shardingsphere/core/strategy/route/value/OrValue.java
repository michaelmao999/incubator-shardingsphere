package org.apache.shardingsphere.core.strategy.route.value;

public class OrValue implements  RouteValueCondition{

    public  static OrValue instance = new OrValue();
    
    public String toString() {
        return " or ";
    }
}
