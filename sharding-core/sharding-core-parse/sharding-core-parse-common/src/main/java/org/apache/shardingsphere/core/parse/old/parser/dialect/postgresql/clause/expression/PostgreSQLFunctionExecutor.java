package org.apache.shardingsphere.core.parse.old.parser.dialect.postgresql.clause.expression;

import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.AbstractSQLFunctionExecutor;

public class PostgreSQLFunctionExecutor extends AbstractSQLFunctionExecutor {

    public PostgreSQLFunctionExecutor(ShardingTableMetaData shardingTableMetaData) {
        super(shardingTableMetaData);
    }

    @Override
    public Object compute(SQLFunctionExpression functionExpression) {
        return null;
    }
}
