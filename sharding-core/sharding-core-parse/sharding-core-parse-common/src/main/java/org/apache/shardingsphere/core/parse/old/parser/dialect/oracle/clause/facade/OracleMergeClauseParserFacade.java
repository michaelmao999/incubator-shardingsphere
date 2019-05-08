package org.apache.shardingsphere.core.parse.old.parser.dialect.oracle.clause.facade;

import org.apache.shardingsphere.core.parse.old.lexer.LexerEngine;
import org.apache.shardingsphere.core.parse.old.parser.clause.*;
import org.apache.shardingsphere.core.parse.old.parser.clause.facade.AbstractMergeClauseParserFacade;
import org.apache.shardingsphere.core.parse.old.parser.dialect.oracle.clause.*;
import org.apache.shardingsphere.core.rule.ShardingRule;
/**
 * MERGE INTO target_table
 * USING source_table
 * ON search_condition
 *     WHEN MATCHED THEN
 *         UPDATE SET col1 = value1, col2 = value2,...
 *         WHERE <update_condition>
 *         [DELETE WHERE <delete_condition>]
 *     WHEN NOT MATCHED THEN
 *         INSERT (col1,col2,...)
 *         values(value1,value2,...)
 *         WHERE <insert_condition>;
 */

public class OracleMergeClauseParserFacade extends AbstractMergeClauseParserFacade {

    public OracleMergeClauseParserFacade(final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        super(//merge into
                new OracleTableReferencesClauseParser(shardingRule, lexerEngine),
                //using
                new OracleSelectListClauseParser(shardingRule, lexerEngine),
                new OracleTableReferencesClauseParser(shardingRule, lexerEngine),
                new OracleWhereClauseParser(lexerEngine), new OracleGroupByClauseParser(lexerEngine),
                new HavingClauseParser(lexerEngine), new OracleOrderByClauseParser(lexerEngine),
                new OracleSelectRestClauseParser(lexerEngine),
                //update
                new OracleTableReferencesClauseParser(shardingRule, lexerEngine),
                new UpdateSetItemsClauseParser(lexerEngine),
                new OracleWhereClauseParser(lexerEngine),
                // delete
                new OracleTableReferencesClauseParser(shardingRule, lexerEngine), new OracleWhereClauseParser(lexerEngine),
                //insert
                new OracleInsertIntoClauseParser(shardingRule, lexerEngine), new InsertColumnsClauseParser(lexerEngine), new OracleInsertValuesClauseParser(shardingRule, lexerEngine),
                new OracleInsertSetClauseParser(shardingRule, lexerEngine), new OracleInsertDuplicateKeyUpdateClauseParser(shardingRule, lexerEngine)
                );
    }
}
