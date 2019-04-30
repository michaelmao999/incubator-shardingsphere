package org.apache.shardingsphere.core.parse.old.parser.dialect.postgresql.clause.expression;

import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExector;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExpression;

import java.util.List;

public class PostgreSQLFunctionExecutor implements SQLFunctionExector {


    @Override
    public Object compute(SQLFunctionExpression functionExpression, List<Object> parameters) {
        return null;
    }
}
