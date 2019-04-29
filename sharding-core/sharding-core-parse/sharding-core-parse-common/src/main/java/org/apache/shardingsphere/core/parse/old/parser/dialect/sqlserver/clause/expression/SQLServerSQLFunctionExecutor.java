package org.apache.shardingsphere.core.parse.old.parser.dialect.sqlserver.clause.expression;

import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.AbstractSQLFunctionExecutor;

public class SQLServerSQLFunctionExecutor extends AbstractSQLFunctionExecutor {

    public SQLServerSQLFunctionExecutor(ShardingTableMetaData shardingTableMetaData) {
        super(shardingTableMetaData);
    }

    @Override
    public Object compute(SQLFunctionExpression functionExpression) {
        return null;
    }
}
