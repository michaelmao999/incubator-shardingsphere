package org.apache.shardingsphere.core.parse.old.parser.dialect.oracle.sql;

import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.old.lexer.LexerEngine;
import org.apache.shardingsphere.core.parse.old.parser.dialect.oracle.clause.facade.OracleMergeClauseParserFacade;
import org.apache.shardingsphere.core.parse.old.parser.sql.dml.merge.AbstractMergeParser;
import org.apache.shardingsphere.core.rule.ShardingRule;

public class OracleMergeParser extends AbstractMergeParser {
    public OracleMergeParser(final ShardingRule shardingRule, final LexerEngine lexerEngine, final ShardingTableMetaData shardingTableMetaData, final String sql) {
        super(shardingRule, lexerEngine, shardingTableMetaData, sql, new OracleMergeClauseParserFacade(shardingRule, lexerEngine),
                new OracleSelectParser(shardingRule, lexerEngine, shardingTableMetaData),
                new OracleUpdateParser(shardingRule, lexerEngine),
                new OracleDeleteParser(shardingRule, lexerEngine),
                new OracleInsertParser(shardingRule, sql, lexerEngine, shardingTableMetaData)
                );
    }
}
