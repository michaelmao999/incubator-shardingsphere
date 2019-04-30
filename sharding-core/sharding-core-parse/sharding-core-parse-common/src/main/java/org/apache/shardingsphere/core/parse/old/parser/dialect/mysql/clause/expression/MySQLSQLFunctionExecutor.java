package org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.clause.expression;

import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExector;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExpression;

import java.util.List;

public class MySQLSQLFunctionExecutor implements SQLFunctionExector {

    @Override
    public Object compute(SQLFunctionExpression functionExpression, List<Object> parameters) {
        return null;
    }
}
