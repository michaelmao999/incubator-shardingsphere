package org.apache.shardingsphere.core.optimize;


import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.core.optimize.engine.NewOptimizeEngine;
import org.apache.shardingsphere.core.optimize.engine.sharding.query.NewQueryOptimizeEngine;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DMLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.SelectStatement;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExector;
import org.apache.shardingsphere.core.rule.ShardingRule;

import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NewOptimizeEngineFactory {
    /**
     * Create optimize engine instance.
     *
     * @param shardingRule sharding rule
     * @param sqlStatement SQL statement
     * @param parameters parameters
     * @param generatedKey generated key
     * @return optimize engine instance
     */
    public static NewOptimizeEngine newInstance(final ShardingRule shardingRule, final SQLStatement sqlStatement, final List<Object> parameters, final GeneratedKey generatedKey, final SQLFunctionExector sqlFunctionExector) {
//        if (sqlStatement instanceof InsertStatement) {
//            return new InsertOptimizeEngine(shardingRule, (InsertStatement) sqlStatement, parameters, generatedKey, sqlFunctionExector);
//        }
        if (sqlStatement instanceof SelectStatement || sqlStatement instanceof DMLStatement) {
            return new NewQueryOptimizeEngine( sqlStatement.getRouteCondition(), parameters, sqlFunctionExector);
        }
        // TODO do with DDL and DAL
        return new NewQueryOptimizeEngine(sqlStatement.getRouteCondition(), parameters, sqlFunctionExector);
    }

}
