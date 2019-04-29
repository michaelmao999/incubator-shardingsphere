package org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.clause.expression;

import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.AbstractSQLFunctionExecutor;

public class MySQLSQLFunctionExecutor extends AbstractSQLFunctionExecutor {

    public MySQLSQLFunctionExecutor(ShardingTableMetaData shardingTableMetaData) {
        super(shardingTableMetaData);
    }

    @Override
    public Object compute(SQLFunctionExpression functionExpression) {
        return null;
    }
}
