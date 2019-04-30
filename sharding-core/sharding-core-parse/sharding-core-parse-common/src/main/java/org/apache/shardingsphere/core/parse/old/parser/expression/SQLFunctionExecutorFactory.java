package org.apache.shardingsphere.core.parse.old.parser.expression;

import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.clause.expression.MySQLSQLFunctionExecutor;
import org.apache.shardingsphere.core.parse.old.parser.dialect.oracle.clause.expression.OracleSQLFunctionExecutor;
import org.apache.shardingsphere.core.parse.old.parser.dialect.postgresql.clause.expression.PostgreSQLFunctionExecutor;
import org.apache.shardingsphere.core.parse.old.parser.dialect.sqlserver.clause.expression.SQLServerSQLFunctionExecutor;

public class SQLFunctionExecutorFactory {

    public static SQLFunctionExector newInstance(final DatabaseType dbType) {
        switch (dbType) {
            case MySQL:
                return new MySQLSQLFunctionExecutor();
            case Oracle:
                return new OracleSQLFunctionExecutor();
            case SQLServer:
                return new SQLServerSQLFunctionExecutor();
            case PostgreSQL:
                return new PostgreSQLFunctionExecutor();
            default:
                throw new UnsupportedOperationException(String.format("Cannot support database type: %s on SQLFunctionExecutor", dbType));
        }
    }
}
