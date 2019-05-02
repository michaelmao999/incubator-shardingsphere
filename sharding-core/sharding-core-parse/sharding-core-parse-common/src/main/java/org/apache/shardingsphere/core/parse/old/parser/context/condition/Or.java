package org.apache.shardingsphere.core.parse.old.parser.context.condition;

public class Or implements SQLCondition, LogicalOperator {
    public static Or instance = new Or();

    public String toString() {
        return " or ";
    }
}
