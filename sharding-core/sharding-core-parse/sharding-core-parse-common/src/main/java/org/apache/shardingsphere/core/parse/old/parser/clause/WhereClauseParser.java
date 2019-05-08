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

package org.apache.shardingsphere.core.parse.old.parser.clause;

import com.google.common.base.Optional;
import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.SelectStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.token.OffsetToken;
import org.apache.shardingsphere.core.parse.antlr.sql.token.RowCountToken;
import org.apache.shardingsphere.core.parse.old.lexer.LexerEngine;
import org.apache.shardingsphere.core.parse.old.lexer.token.*;
import org.apache.shardingsphere.core.parse.old.parser.clause.condition.NullCondition;
import org.apache.shardingsphere.core.parse.old.parser.clause.expression.AliasExpressionParser;
import org.apache.shardingsphere.core.parse.old.parser.clause.expression.BasicExpressionParser;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.*;
import org.apache.shardingsphere.core.parse.old.parser.context.limit.Limit;
import org.apache.shardingsphere.core.parse.old.parser.context.limit.LimitValue;
import org.apache.shardingsphere.core.parse.old.parser.context.selectitem.SelectItem;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Table;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Tables;
import org.apache.shardingsphere.core.parse.old.parser.dialect.ExpressionParserFactory;
import org.apache.shardingsphere.core.parse.old.parser.expression.*;
import org.apache.shardingsphere.core.parse.util.SQLUtil;
import org.apache.shardingsphere.core.rule.ShardingRule;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Where clause parser.
 *
 * @author zhangliang
 * @author maxiaoguang
 */
public abstract class WhereClauseParser implements SQLClauseParser {
    
    private final DatabaseType databaseType;
    
    private final LexerEngine lexerEngine;
    
    private final AliasExpressionParser aliasExpressionParser;
    
    private final BasicExpressionParser basicExpressionParser;

    private static Keyword[] cachedOtherConditionOperators;

    public WhereClauseParser(final DatabaseType databaseType, final LexerEngine lexerEngine) {
        this.databaseType = databaseType;
        this.lexerEngine = lexerEngine;
        aliasExpressionParser = ExpressionParserFactory.createAliasExpressionParser(lexerEngine);
        basicExpressionParser = ExpressionParserFactory.createBasicExpressionParser(lexerEngine);
    }

    /**
     * Parse where.
     *
     * @param shardingRule databases and tables sharding rule
     * @param sqlStatement SQL statement
     * @param items select items
     */
    public void parse(final ShardingRule shardingRule, final SQLStatement sqlStatement, final List<SelectItem> items, boolean isSubGroup) {
        aliasExpressionParser.parseTableAlias();
        if (lexerEngine.skipIfEqualType(DefaultKeyword.WHERE)) {
            parseWhere(shardingRule, sqlStatement, items, isSubGroup);
        }
    }
    
    public void parseWhere(final ShardingRule shardingRule, final SQLStatement sqlStatement, final List<SelectItem> items, boolean isSubGroup) {
        Group group = parseGroup(shardingRule, sqlStatement, items, isSubGroup)
                .optimize();
        if (group.size() > 0) {
            sqlStatement.getRouteCondition().add(group);
        }
    }


    private Group parseGroup(final ShardingRule shardingRule, final SQLStatement sqlStatement, final List<SelectItem> items, boolean isSubGroup) {
        Group result = new Group();
        do {
            if (lexerEngine.isEnd()) {
                break;
            }
            TokenType tokenType = lexerEngine.getCurrentToken().getType();
            if (tokenType == DefaultKeyword.GROUP || tokenType == DefaultKeyword.ORDER
                    || tokenType == DefaultKeyword.UNION || tokenType == DefaultKeyword.MINUS
                    || tokenType == DefaultKeyword.WHEN   //merge into table using table2 on condition when match
                    || (isSubGroup && tokenType == Symbol.RIGHT_PAREN)) {
                break;
            }

            if (lexerEngine.skipIfEqualType(Symbol.LEFT_PAREN)) {
                Group subOrCondition = parseGroup(shardingRule, sqlStatement, items, true);
                lexerEngine.skipIfEqualType(Symbol.RIGHT_PAREN);
                result.add(subOrCondition);
            } else if (lexerEngine.skipIfEqualType(DefaultKeyword.OR)){
                result.add(Or.instance);
            } else if (lexerEngine.skipIfEqualType(DefaultKeyword.AND)){
                result.add(And.instance);
            } else {
                Condition condition = parseComparisonCondition(shardingRule, sqlStatement, items);
                result.add(condition);
                skipsDoubleColon();
            }
        } while (true);
        return result;
    }

    private Keyword[] getCachedOtherConditionOperators() {
        if (cachedOtherConditionOperators == null) {
            List<Keyword> otherConditionOperators = new LinkedList<>(Arrays.asList(getCustomizedOtherConditionOperators()));
            otherConditionOperators.addAll(
                    Arrays.asList(Symbol.LT, Symbol.LT_EQ, Symbol.GT, Symbol.GT_EQ, Symbol.LT_GT, Symbol.BANG_EQ, Symbol.BANG_GT, Symbol.BANG_LT, DefaultKeyword.LIKE, DefaultKeyword.IS));
            cachedOtherConditionOperators = otherConditionOperators.toArray(new Keyword[otherConditionOperators.size()]);
        }
        return cachedOtherConditionOperators;
    }
    
    private Condition parseComparisonCondition(final ShardingRule shardingRule, final SQLStatement sqlStatement, final List<SelectItem> items) {
        Condition result;
        SQLExpression left = basicExpressionParser.parse(sqlStatement);
        if (lexerEngine.skipIfEqualType(Symbol.EQ)) {
            result = parseEqualCondition(shardingRule, sqlStatement, left, Symbol.EQ);
            return result;
        }
        if (lexerEngine.skipIfEqualType(Symbol.GT)) {
            result = parseEqualCondition(shardingRule, sqlStatement, left, Symbol.GT);
            return result;
        }
        if (lexerEngine.skipIfEqualType(Symbol.LT)) {
            result = parseEqualCondition(shardingRule, sqlStatement, left, Symbol.LT);
            return result;
        }
        if (lexerEngine.skipIfEqualType(Symbol.GT_EQ)) {
            result = parseEqualCondition(shardingRule, sqlStatement, left, Symbol.GT_EQ);
            return result;
        }
        if (lexerEngine.skipIfEqualType(Symbol.LT_EQ)) {
            result = parseEqualCondition(shardingRule, sqlStatement, left, Symbol.LT_EQ);
            return result;
        }
        if (lexerEngine.skipIfEqualType(DefaultKeyword.IN)) {
            result = parseInCondition(shardingRule, sqlStatement, left);
            return result;
        }
        if (lexerEngine.skipIfEqualType(DefaultKeyword.BETWEEN)) {
            result = parseBetweenCondition(shardingRule, sqlStatement, left);
            return result;
        }
        result = NullCondition.instance;
        if (sqlStatement instanceof SelectStatement && isRowNumberCondition(items, left)) {
            if (lexerEngine.skipIfEqualType(Symbol.LT)) {
                parseRowCountCondition((SelectStatement) sqlStatement, false);
                return result;
            }
            if (lexerEngine.skipIfEqualType(Symbol.LT_EQ)) {
                parseRowCountCondition((SelectStatement) sqlStatement, true);
                return result;
            }
            if (lexerEngine.skipIfEqualType(Symbol.GT)) {
                parseOffsetCondition((SelectStatement) sqlStatement, false);
                return result;
            }
            if (lexerEngine.skipIfEqualType(Symbol.GT_EQ)) {
                parseOffsetCondition((SelectStatement) sqlStatement, true);
                return result;
            }
        }
        if (lexerEngine.skipIfEqual(getCachedOtherConditionOperators())) {
            lexerEngine.skipIfEqualType(DefaultKeyword.NOT);
            parseOtherCondition(sqlStatement);
        }
        if (lexerEngine.skipIfEqualType(DefaultKeyword.NOT)) {
            parseNotCondition(sqlStatement);
        }
        return result;
    }
    
    private Condition parseEqualCondition(final ShardingRule shardingRule, final SQLStatement sqlStatement, final SQLExpression left, final Symbol operator) {
        SQLExpression right = basicExpressionParser.parse(sqlStatement);
        // TODO if have more tables, and cannot find column belong to, should not add to condition, should parse binding table rule.
        if (!sqlStatement.getTables().isSingleTable() && !(left instanceof SQLPropertyExpression)) {
            return NullCondition.instance;
        }
        if (right instanceof SQLNumberExpression || right instanceof SQLTextExpression || right instanceof SQLParameterMarkerExpression || right instanceof  SQLFunctionExpression) {
            Optional<Column> column = find(sqlStatement.getTables(), left);
            if (column.isPresent() && shardingRule.isShardingColumn(column.get().getName(), column.get().getTableName())) {
                return new Condition(column.get(), operator.getLiterals(), right);
            }
        }
        return NullCondition.instance;
    }
    
    private Condition parseInCondition(final ShardingRule shardingRule, final SQLStatement sqlStatement, final SQLExpression left) {
        lexerEngine.accept(Symbol.LEFT_PAREN);
        boolean hasComplexExpression = false;
        List<SQLExpression> rights = new LinkedList<>();
        do {
            SQLExpression right = basicExpressionParser.parse(sqlStatement);
            rights.add(right);
            if (!(right instanceof SQLNumberExpression || right instanceof SQLTextExpression || right instanceof SQLParameterMarkerExpression)) {
                hasComplexExpression = true;
            }
            skipsDoubleColon();
        } while (lexerEngine.skipIfEqualType(Symbol.COMMA));
        lexerEngine.accept(Symbol.RIGHT_PAREN);
        if (!sqlStatement.getTables().isSingleTable() && !(left instanceof SQLPropertyExpression)) {
            return new NullCondition();
        }
        if (!hasComplexExpression) {
            Optional<Column> column = find(sqlStatement.getTables(), left);
            if (column.isPresent() && shardingRule.isShardingColumn(column.get().getName(), column.get().getTableName())) {
                return new Condition(column.get(), rights);
            }
        }
        return new NullCondition();
    }
    
    private Condition parseBetweenCondition(final ShardingRule shardingRule, final SQLStatement sqlStatement, final SQLExpression left) {
        boolean hasComplexExpression = false;
        SQLExpression right1 = basicExpressionParser.parse(sqlStatement);
        if (!(right1 instanceof SQLNumberExpression || right1 instanceof SQLTextExpression
                || right1 instanceof SQLParameterMarkerExpression || right1 instanceof SQLFunctionExpression)) {
            hasComplexExpression = true;
        }
        skipsDoubleColon();
        lexerEngine.accept(DefaultKeyword.AND);
        SQLExpression right2 = basicExpressionParser.parse(sqlStatement);
        if (!(right2 instanceof SQLNumberExpression || right2 instanceof SQLTextExpression
                || right2 instanceof SQLParameterMarkerExpression || right2 instanceof SQLFunctionExpression)) {
            hasComplexExpression = true;
        }
        if (!sqlStatement.getTables().isSingleTable() && !(left instanceof SQLPropertyExpression)) {
            return new NullCondition();
        }
        if (!hasComplexExpression) {
            Optional<Column> column = find(sqlStatement.getTables(), left);
            if (column.isPresent() && shardingRule.isShardingColumn(column.get().getName(), column.get().getTableName())) {
                return new Condition(column.get(), right1, right2);
            }
        }
        return new NullCondition();
    }
    
    private boolean isRowNumberCondition(final List<SelectItem> items, final SQLExpression sqlExpression) {
        String columnLabel = null;
        if (sqlExpression instanceof SQLIdentifierExpression) {
            columnLabel = ((SQLIdentifierExpression) sqlExpression).getName();
        } else if (sqlExpression instanceof SQLPropertyExpression) {
            columnLabel = ((SQLPropertyExpression) sqlExpression).getName();
        }
        return null != columnLabel && isRowNumberCondition(items, columnLabel);
    }
    
    protected abstract boolean isRowNumberCondition(List<SelectItem> items, String columnLabel);
    
    private void parseRowCountCondition(final SelectStatement selectStatement, final boolean includeRowCount) {
        int endPosition = lexerEngine.getCurrentToken().getEndPosition();
        SQLExpression sqlExpression = basicExpressionParser.parse(selectStatement);
        if (null == selectStatement.getLimit()) {
            selectStatement.setLimit(new Limit());
        }
        if (sqlExpression instanceof SQLNumberExpression) {
            int rowCount = ((SQLNumberExpression) sqlExpression).getNumber().intValue();
            selectStatement.getLimit().setRowCount(new LimitValue(rowCount, -1, includeRowCount));
            selectStatement.addSQLToken(new RowCountToken(endPosition - String.valueOf(rowCount).length(), endPosition - 1, rowCount));
        } else if (sqlExpression instanceof SQLParameterMarkerExpression) {
            selectStatement.getLimit().setRowCount(new LimitValue(-1, ((SQLParameterMarkerExpression) sqlExpression).getIndex(), includeRowCount));
        }
    }
    
    private void parseOffsetCondition(final SelectStatement selectStatement, final boolean includeOffset) {
        SQLExpression sqlExpression = basicExpressionParser.parse(selectStatement);
        if (null == selectStatement.getLimit()) {
            selectStatement.setLimit(new Limit());
        }
        if (sqlExpression instanceof SQLNumberExpression) {
            int offset = ((SQLNumberExpression) sqlExpression).getNumber().intValue();
            selectStatement.getLimit().setOffset(new LimitValue(offset, -1, includeOffset));
            selectStatement.addSQLToken(new OffsetToken(lexerEngine.getCurrentToken().getEndPosition() - String.valueOf(offset).length() - lexerEngine.getCurrentToken().getLiterals().length(),
                    lexerEngine.getCurrentToken().getEndPosition() - 1, offset));
        } else if (sqlExpression instanceof SQLParameterMarkerExpression) {
            selectStatement.getLimit().setOffset(new LimitValue(-1, ((SQLParameterMarkerExpression) sqlExpression).getIndex(), includeOffset));
        }
    }
    
    protected abstract Keyword[] getCustomizedOtherConditionOperators();
    
    private void parseOtherCondition(final SQLStatement sqlStatement) {
        basicExpressionParser.parse(sqlStatement);
    }
    
    private void parseNotCondition(final SQLStatement sqlStatement) {
        if (lexerEngine.skipIfEqualType(DefaultKeyword.BETWEEN)) {
            parseOtherCondition(sqlStatement);
            skipsDoubleColon();
            lexerEngine.accept(DefaultKeyword.AND);
            parseOtherCondition(sqlStatement);
            return;
        }
        if (lexerEngine.skipIfEqualType(DefaultKeyword.IN)) {
            lexerEngine.accept(Symbol.LEFT_PAREN);
            do {
                parseOtherCondition(sqlStatement);
                skipsDoubleColon();
            } while (lexerEngine.skipIfEqualType(Symbol.COMMA));
            lexerEngine.accept(Symbol.RIGHT_PAREN);
        } else {
            lexerEngine.nextToken();
            parseOtherCondition(sqlStatement);
        }
    }
    
    private Optional<Column> find(final Tables tables, final SQLExpression sqlExpression) {
        if (sqlExpression instanceof SQLPropertyExpression) {
            return getColumnWithOwner(tables, (SQLPropertyExpression) sqlExpression);
        }
        if (sqlExpression instanceof SQLIdentifierExpression) {
            return getColumnWithoutOwner(tables, (SQLIdentifierExpression) sqlExpression);
        }
        return Optional.absent();
    }
    
    private Optional<Column> getColumnWithOwner(final Tables tables, final SQLPropertyExpression propertyExpression) {
        Optional<Table> table = tables.find(SQLUtil.getExactlyValue((propertyExpression.getOwner()).getName()));
        return table.isPresent() ? Optional.of(new Column(SQLUtil.getExactlyValue(propertyExpression.getName()), table.get().getName())) : Optional.<Column>absent();
    }
    
    private Optional<Column> getColumnWithoutOwner(final Tables tables, final SQLIdentifierExpression identifierExpression) {
        return tables.isSingleTable() ? Optional.of(new Column(SQLUtil.getExactlyValue(identifierExpression.getName()), tables.getSingleTableName())) : Optional.<Column>absent();
    }
    
    private void skipsDoubleColon() {
        if (lexerEngine.skipIfEqualType(Symbol.DOUBLE_COLON)) {
            lexerEngine.nextToken();
        }
    }
}
