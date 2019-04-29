/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.core.parse.old.parser.dialect.oracle.clause;

import org.apache.shardingsphere.core.parse.antlr.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.parse.old.lexer.LexerEngine;
import org.apache.shardingsphere.core.parse.old.lexer.dialect.oracle.OracleKeyword;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;
import org.apache.shardingsphere.core.parse.old.lexer.token.Symbol;
import org.apache.shardingsphere.core.parse.old.parser.clause.TableReferencesClauseParser;
import org.apache.shardingsphere.core.rule.ShardingRule;

/**
 * Table references clause parser for Oracle.
 *
 * @author zhangliang
 */
public final class OracleTableReferencesClauseParser extends TableReferencesClauseParser {
    
    public OracleTableReferencesClauseParser(final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        super(shardingRule, lexerEngine);
    }
    
    @Override
    protected void parseTableReference(final SQLStatement sqlStatement, final boolean isSingleTableOnly) {
        if (getLexerEngine().skipIfEqualType(OracleKeyword.ONLY)) {
            getLexerEngine().skipIfEqualType(Symbol.LEFT_PAREN);
            parseQueryTableExpression(sqlStatement, isSingleTableOnly);
            getLexerEngine().skipIfEqualType(Symbol.RIGHT_PAREN);
            parseFlashbackQueryClause();
        } else {
            parseQueryTableExpression(sqlStatement, isSingleTableOnly);
            parsePivotClause(sqlStatement);
            parseFlashbackQueryClause();
        }
    }
    
    private void parseQueryTableExpression(final SQLStatement sqlStatement, final boolean isSingleTableOnly) {
        parseTableFactor(sqlStatement, isSingleTableOnly);
        parseDbLink();
        parsePartitionExtensionClause();
        parseSampleClause();
    }
    
    private void parseDbLink() {
        getLexerEngine().unsupportedIfEqual(Symbol.AT);
    }
    
    private void parsePartitionExtensionClause() {
        getLexerEngine().unsupportedIfEqual(OracleKeyword.PARTITION, OracleKeyword.SUBPARTITION);
    }
    
    private void parseSampleClause() {
        getLexerEngine().unsupportedIfEqual(OracleKeyword.SAMPLE);
    }
    
    private void parseFlashbackQueryClause() {
        if (isFlashbackQueryClauseForVersions() || isFlashbackQueryClauseForAs()) {
            throw new UnsupportedOperationException("Cannot support Flashback Query");
        }
    }
    
    private boolean isFlashbackQueryClauseForVersions() {
        return getLexerEngine().skipIfEqualType(OracleKeyword.VERSIONS) && getLexerEngine().skipIfEqualType(DefaultKeyword.BETWEEN);
    }
    
    private boolean isFlashbackQueryClauseForAs() {
        return getLexerEngine().skipIfEqualType(DefaultKeyword.AS) && getLexerEngine().skipIfEqualType(DefaultKeyword.OF)
                && (getLexerEngine().skipIfEqualType(OracleKeyword.SCN) || getLexerEngine().skipIfEqualType(OracleKeyword.TIMESTAMP));
    }
    
    private void parsePivotClause(final SQLStatement sqlStatement) {
        if (getLexerEngine().skipIfEqualType(OracleKeyword.PIVOT)) {
            getLexerEngine().skipIfEqualType(OracleKeyword.XML);
            getLexerEngine().skipParentheses(sqlStatement);
        } else if (getLexerEngine().skipIfEqualType(OracleKeyword.UNPIVOT)) {
            if (getLexerEngine().skipIfEqualType(OracleKeyword.INCLUDE)) {
                getLexerEngine().accept(OracleKeyword.NULLS);
            } else if (getLexerEngine().skipIfEqualType(OracleKeyword.EXCLUDE)) {
                getLexerEngine().accept(OracleKeyword.NULLS);
            }
            getLexerEngine().skipParentheses(sqlStatement);
        }
    }
}
