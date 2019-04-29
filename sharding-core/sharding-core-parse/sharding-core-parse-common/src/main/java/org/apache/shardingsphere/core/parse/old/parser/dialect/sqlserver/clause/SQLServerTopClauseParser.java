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

package org.apache.shardingsphere.core.parse.old.parser.dialect.sqlserver.clause;

import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.SelectStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.token.RowCountToken;
import org.apache.shardingsphere.core.parse.old.lexer.LexerEngine;
import org.apache.shardingsphere.core.parse.old.lexer.dialect.sqlserver.SQLServerKeyword;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;
import org.apache.shardingsphere.core.parse.old.lexer.token.Symbol;
import org.apache.shardingsphere.core.parse.old.parser.clause.SQLClauseParser;
import org.apache.shardingsphere.core.parse.old.parser.clause.expression.BasicExpressionParser;
import org.apache.shardingsphere.core.parse.old.parser.context.limit.Limit;
import org.apache.shardingsphere.core.parse.old.parser.context.limit.LimitValue;
import org.apache.shardingsphere.core.parse.old.parser.dialect.ExpressionParserFactory;
import org.apache.shardingsphere.core.parse.old.parser.exception.SQLParsingException;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLNumberExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLPlaceholderExpression;

/**
 * Top clause parser for SQLServer.
 *
 * @author zhangliang
 */
public final class SQLServerTopClauseParser implements SQLClauseParser {
    
    private final LexerEngine lexerEngine;
    
    private final BasicExpressionParser basicExpressionParser;
    
    public SQLServerTopClauseParser(final LexerEngine lexerEngine) {
        this.lexerEngine = lexerEngine;
        basicExpressionParser = ExpressionParserFactory.createBasicExpressionParser(lexerEngine);
    }
    
    /**
     * Parse top.
     * 
     * @param selectStatement select statement
     */
    public void parse(final SelectStatement selectStatement) {
        if (!lexerEngine.skipIfEqualType(SQLServerKeyword.TOP)) {
            return;
        }
        int beginPosition = lexerEngine.getCurrentToken().getEndPosition();
        if (!lexerEngine.skipIfEqualType(Symbol.LEFT_PAREN)) {
            beginPosition = lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length();
        }
        SQLExpression sqlExpression = basicExpressionParser.parse(selectStatement);
        lexerEngine.skipIfEqualType(Symbol.RIGHT_PAREN);
        LimitValue rowCountValue;
        if (sqlExpression instanceof SQLNumberExpression) {
            int rowCount = ((SQLNumberExpression) sqlExpression).getNumber().intValue();
            rowCountValue = new LimitValue(rowCount, -1, false);
            selectStatement.addSQLToken(new RowCountToken(beginPosition, rowCount));
        } else if (sqlExpression instanceof SQLPlaceholderExpression) {
            rowCountValue = new LimitValue(-1, ((SQLPlaceholderExpression) sqlExpression).getIndex(), false);
        } else {
            throw new SQLParsingException(lexerEngine);
        }
        lexerEngine.unsupportedIfEqual(SQLServerKeyword.PERCENT);
        lexerEngine.skipIfEqual(DefaultKeyword.WITH, SQLServerKeyword.TIES);
        if (null == selectStatement.getLimit()) {
            Limit limit = new Limit();
            limit.setRowCount(rowCountValue);
            selectStatement.setLimit(limit);
        } else {
            selectStatement.getLimit().setRowCount(rowCountValue);
        }
    }
}
