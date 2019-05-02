package org.apache.shardingsphere.core.parse.old.parser.context.condition;

public class And implements SQLCondition, LogicalOperator {

    public static And instance = new And();
    public String toString() {
        return " and ";
    }
}
