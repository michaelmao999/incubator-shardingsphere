package org.apache.shardingsphere.core.parse.old.parser.expression;

import java.util.List;

public interface SQLFunctionExector {
    Object compute(SQLFunctionExpression functionExpression, final List<Object> parameters);
}
