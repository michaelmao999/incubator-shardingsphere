package org.apache.shardingsphere.core.parse.old.parser.dialect.sqlserver.clause.expression;

import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExector;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExpression;

import java.util.List;

public class SQLServerSQLFunctionExecutor implements SQLFunctionExector {


    @Override
    public Object compute(SQLFunctionExpression functionExpression, List<Object> parameters) {
        return null;
    }
}
