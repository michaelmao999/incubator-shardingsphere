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

package org.apache.shardingsphere.core.parse.old.parser.clause.expression;

import com.google.common.base.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.parse.antlr.constant.QuoteCharacter;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.token.TableToken;
import org.apache.shardingsphere.core.parse.old.lexer.LexerEngine;
import org.apache.shardingsphere.core.parse.old.lexer.dialect.oracle.OracleKeyword;
import org.apache.shardingsphere.core.parse.old.lexer.token.*;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Table;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Tables;
import org.apache.shardingsphere.core.parse.old.parser.expression.*;
import org.apache.shardingsphere.core.parse.util.SQLUtil;
import org.apache.shardingsphere.core.util.NumberUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Basic expression parser.
 *
 * @author zhangliang
 * @author panjuan
 */
@RequiredArgsConstructor
public final class BasicExpressionParser {
    
    private final LexerEngine lexerEngine;
    
    /**
     * Parse expression.
     *
     * @param sqlStatement SQL statement
     * @return expression
     */
    public SQLExpression parse(final SQLStatement sqlStatement) {
        return parse(sqlStatement, false);
    }

    public SQLExpression parse(final SQLStatement sqlStatement, boolean isInFunction) {
        int beginPosition = lexerEngine.getCurrentToken().getEndPosition();
        SQLExpression result = parseExpression(sqlStatement, isInFunction);
        if (result instanceof SQLPropertyExpression) {
            setTableToken(sqlStatement, beginPosition, (SQLPropertyExpression) result);
        }
        return result;
    }

    
    // TODO complete more expression parse
    private SQLExpression parseExpression(final SQLStatement sqlStatement, boolean isInFunction) {
        String literals = lexerEngine.getCurrentToken().getLiterals();
        final int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - literals.length();
        final SQLExpression expression = getExpression(literals, sqlStatement);
        lexerEngine.nextToken();
        if (lexerEngine.skipIfEqualType(Symbol.DOT)) {
            String property = lexerEngine.getCurrentToken().getLiterals();
            lexerEngine.nextToken();
            return skipIfCompositeExpression(sqlStatement)
                    ? new SQLIgnoreExpression(lexerEngine.getInput().substring(beginPosition, lexerEngine.getCurrentToken().getEndPosition()))
                    : new SQLPropertyExpression(new SQLIdentifierExpression(literals), property);
        }
        //Parse SQL function, such as to_date('05-01-2019','MM-DD-YYYY')
        if (lexerEngine.equalOne(Symbol.LEFT_PAREN) && expression instanceof SQLIdentifierExpression) {
            return parseFunction(((SQLIdentifierExpression)expression).getName(), sqlStatement );
        }
        String literal2 = lexerEngine.getCurrentToken().getLiterals();
        if (expression instanceof SQLIdentifierExpression
                && (lexerEngine.equalAny(Symbol.PLUS, Symbol.SUB)
                || ((literal2.startsWith("+") || literal2.startsWith("-") ))
                    && lexerEngine.getCurrentToken().getType() == Literals.INT)) {
            List<SQLExpression> parameters = new ArrayList<SQLExpression>();
            parameters.add(expression);
            return parsePureExpression(parameters, sqlStatement);
        }
        if (lexerEngine.equalOne(Symbol.LEFT_PAREN)) {
            lexerEngine.skipParentheses(sqlStatement);
            skipRestCompositeExpression(sqlStatement);
            return new SQLIgnoreExpression(lexerEngine.getInput().substring(beginPosition,
                    lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length()).trim());
        }
        return skipIfCompositeExpression(sqlStatement)
                ? new SQLIgnoreExpression(lexerEngine.getInput().substring(beginPosition, lexerEngine.getCurrentToken().getEndPosition())) : expression;
    }

    public SQLFunctionExpression parseFunction(final String functionName, final SQLStatement sqlStatement) {
        List<SQLExpression> parameters = new ArrayList<SQLExpression>();
        int count = 0;
        if (Symbol.LEFT_PAREN == lexerEngine.getCurrentToken().getType()) {
            while (true) {
                lexerEngine.nextToken();
                SQLExpression parameter = parse(sqlStatement, true);
                parameters.add(parameter);
                if (lexerEngine.equalOne(Symbol.COMMA)) {
                    continue;
                }
                if (lexerEngine.equalOne(Symbol.QUESTION)) {
                    sqlStatement.setParametersIndex(sqlStatement.getParametersIndex() + 1);
                }
                if (Assist.END == lexerEngine.getCurrentToken().getType() || (Symbol.RIGHT_PAREN == lexerEngine.getCurrentToken().getType() && 0 == count)) {
                    break;
                }
                if (Symbol.LEFT_PAREN == lexerEngine.getCurrentToken().getType()) {
                    count++;
                } else if (Symbol.RIGHT_PAREN == lexerEngine.getCurrentToken().getType()) {
                    count--;
                }

            }
            lexerEngine.nextToken();
        }
        return new SQLFunctionExpression(functionName.toLowerCase(), parameters);
    }

    public SQLFunctionExpression parsePureExpression(final List<SQLExpression> parameters, final SQLStatement sqlStatement) {
        int count = 0;
        while (true) {
            Token currentToken = lexerEngine.getCurrentToken();
            final SQLExpression expression = getExpression(currentToken.getLiterals(), sqlStatement);
            parameters.add(expression);
            lexerEngine.nextToken();
            if (lexerEngine.getCurrentToken().getType() == Symbol.COMMA ||
                    Assist.END == lexerEngine.getCurrentToken().getType()
                    || (Symbol.RIGHT_PAREN == lexerEngine.getCurrentToken().getType() && 0 == count)) {
                break;
            }
        }
        return new SQLFunctionExpression(null, parameters);
    }


    private SQLExpression getExpression(final String literals, final SQLStatement sqlStatement) {
        if (lexerEngine.equalOne(Symbol.QUESTION)) {
            sqlStatement.setParametersIndex(sqlStatement.getParametersIndex() + 1);
            return new SQLParameterMarkerExpression(sqlStatement.getParametersIndex() - 1);
        }
        if (lexerEngine.equalOne(Literals.CHARS)) {
            return new SQLTextExpression(literals);
        }
        if (lexerEngine.equalOne(Literals.INT)) {
            return new SQLNumberExpression(NumberUtil.getExactlyNumber(literals, 10));
        }
        if (lexerEngine.equalOne(Literals.FLOAT)) {
            return new SQLNumberExpression(Double.parseDouble(literals));
        }
        if (lexerEngine.equalOne(Literals.HEX)) {
            return new SQLNumberExpression(NumberUtil.getExactlyNumber(literals, 16));
        }

        if (lexerEngine.equalOne(DefaultKeyword.CASE)) {
            return new SQLIgnoreExpression(parseCaseWhenExpression());
        }
        boolean isKeyword = lexerEngine.getCurrentToken().getType() instanceof DefaultKeyword;
        isKeyword = isKeyword || lexerEngine.getCurrentToken().getType() instanceof Symbol;
        isKeyword = isKeyword || lexerEngine.getCurrentToken().getType() instanceof OracleKeyword;
        if (lexerEngine.equalOne(Literals.IDENTIFIER )|| isKeyword) {
            return new SQLIdentifierExpression(SQLUtil.getExactlyValue(literals));
        }
        return new SQLIgnoreExpression(literals);
    }

    private String parseCaseWhenExpression() {
        StringBuilder result = new StringBuilder();
        int beginPosition = lexerEngine.getCurrentToken().getEndPosition();
        result.append(lexerEngine.getCurrentToken().getLiterals()).append(" ");
        int level = 0;
        lexerEngine.nextToken();
        while (true) {
            if (lexerEngine.equalOne(DefaultKeyword.END) && level == 0 ) {
                break;
            }
            if (lexerEngine.equalOne(DefaultKeyword.CASE)) {
                level ++;
            }
            if (lexerEngine.equalOne(DefaultKeyword.END)) {
                level --;
            }
            lexerEngine.nextToken();
        }
        result.append(lexerEngine.getInput().substring(beginPosition, lexerEngine.getCurrentToken().getEndPosition()));
        return result.toString();

    }


    private boolean skipIfCompositeExpression(final SQLStatement sqlStatement) {
        if (lexerEngine.equalAny(
                Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH, Symbol.PERCENT, Symbol.AMP, Symbol.BAR, Symbol.DOUBLE_AMP, Symbol.DOUBLE_BAR, Symbol.CARET, Symbol.DOT, Symbol.LEFT_PAREN)) {
            lexerEngine.skipParentheses(sqlStatement);
            skipRestCompositeExpression(sqlStatement);
            return true;
        }
        if ((Literals.INT == lexerEngine.getCurrentToken().getType() || Literals.FLOAT == lexerEngine.getCurrentToken().getType()) && lexerEngine.getCurrentToken().getLiterals().startsWith("-")) {
            lexerEngine.nextToken();
            return true;
        }
        return false;
    }
    
    private void skipRestCompositeExpression(final SQLStatement sqlStatement) {
        while (lexerEngine.skipIfEqual(Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH, Symbol.PERCENT, Symbol.AMP, Symbol.BAR, Symbol.DOUBLE_AMP, Symbol.DOUBLE_BAR, Symbol.CARET, Symbol.DOT)) {
            if (lexerEngine.equalOne(Symbol.QUESTION)) {
                sqlStatement.setParametersIndex(sqlStatement.getParametersIndex() + 1);
            }
            lexerEngine.nextToken();
            lexerEngine.skipParentheses(sqlStatement);
        }
    }
    
    private void setTableToken(final SQLStatement sqlStatement, final int beginPosition, final SQLPropertyExpression propertyExpr) {
        String owner = propertyExpr.getOwner().getName();
        Tables tables = sqlStatement.getTables();
        if (tables.getTableNames().contains(SQLUtil.getExactlyValue(owner))) {
            sqlStatement.addSQLToken(new TableToken(beginPosition - owner.length(), beginPosition - 1, owner, QuoteCharacter.getQuoteCharacter(owner)));
        } else if (tables.getTableAliases().contains(SQLUtil.getExactlyValue(owner))){
            Optional<Table> tableOptional = tables.findTableFromAlias(owner);
            if (tableOptional.isPresent()) {
                String tableName = tableOptional.get().getName();
                sqlStatement.addSQLToken(new TableToken(beginPosition - owner.length(), beginPosition - 1, owner, QuoteCharacter.getQuoteCharacter(owner)));

            }
        }
    }
}
