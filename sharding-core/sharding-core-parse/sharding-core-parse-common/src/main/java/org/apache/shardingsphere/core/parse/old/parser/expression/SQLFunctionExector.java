package org.apache.shardingsphere.core.parse.old.parser.expression;

import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExpression;

public interface SQLFunctionExector {
    Object compute(SQLFunctionExpression functionExpression);
}
