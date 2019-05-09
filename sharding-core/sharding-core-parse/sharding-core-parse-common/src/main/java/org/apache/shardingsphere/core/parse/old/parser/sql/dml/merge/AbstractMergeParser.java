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

package org.apache.shardingsphere.core.parse.old.parser.sql.dml.merge;


import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.*;
import org.apache.shardingsphere.core.parse.old.lexer.LexerEngine;
import org.apache.shardingsphere.core.parse.old.lexer.dialect.oracle.OracleKeyword;
import org.apache.shardingsphere.core.parse.old.lexer.token.Assist;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;
import org.apache.shardingsphere.core.parse.old.lexer.token.Literals;
import org.apache.shardingsphere.core.parse.old.lexer.token.Symbol;
import org.apache.shardingsphere.core.parse.old.parser.clause.facade.AbstractMergeClauseParserFacade;
import org.apache.shardingsphere.core.parse.old.parser.context.selectitem.SelectItem;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Table;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Tables;
import org.apache.shardingsphere.core.parse.old.parser.sql.SQLParser;
import org.apache.shardingsphere.core.parse.old.parser.sql.dml.delete.AbstractDeleteParser;
import org.apache.shardingsphere.core.parse.old.parser.sql.dml.insert.AbstractInsertParser;
import org.apache.shardingsphere.core.parse.old.parser.sql.dml.select.AbstractSelectParser;
import org.apache.shardingsphere.core.parse.old.parser.sql.dml.update.AbstractUpdateParser;
import org.apache.shardingsphere.core.rule.ShardingRule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Merge parser.
 * 
 * @author Michael Mao
 */
@RequiredArgsConstructor
@Getter(AccessLevel.PROTECTED)
public abstract class AbstractMergeParser implements SQLParser {

    @Getter(AccessLevel.PROTECTED)
    private final ShardingRule shardingRule;

    @Getter(AccessLevel.PROTECTED)
    private final LexerEngine lexerEngine;

    private final ShardingTableMetaData shardingTableMetaData;

    private final String sql;

    private final AbstractMergeClauseParserFacade mergeClauseParserFacade;

    private final AbstractSelectParser selectParser;

    private final AbstractUpdateParser updateParser;

    private final AbstractDeleteParser deleteParser;

    private final AbstractInsertParser insertParser;

    /**
     * Using select clause.
     */
    private final List<SelectItem> items = new LinkedList<>();
    

    
    @Override
    public final MergeStatement parse() {
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
        MergeStatement result = new MergeStatement();
        lexerEngine.nextToken();
        parseMergeIno(result);
        parseMergeUsing(result);
        parseMergeOnCondition(result);
        parseMergeWhenMatch(result);
        parseMergeWhenMatch(result);
        return result;
    }

    private void parseMergeIno(MergeStatement mergeStatement) {
        if (lexerEngine.skipIfEqualType(DefaultKeyword.INTO)) {
            parseTable(mergeStatement);
        }
    }

    private void parseMergeUsing(MergeStatement mergeStatement) {
        if (lexerEngine.skipIfEqualType(DefaultKeyword.USING)) {
            if (lexerEngine.skipIfEqualType(Symbol.LEFT_PAREN)) {
                if (lexerEngine.getCurrentToken().getType() == DefaultKeyword.SELECT) {
                    SelectStatement selectStatement = selectParser.parse(true);
                    mergeStatement.setUsingSelectStatement(selectStatement);
                    lexerEngine.skipIfEqualType(Symbol.RIGHT_PAREN);
                    if (lexerEngine.getCurrentToken().getType().equals(Literals.IDENTIFIER)) {
                        String alias = lexerEngine.getCurrentToken().getLiterals();
                        mergeStatement.setSelectAlias(alias);
                        lexerEngine.nextToken();
                    }
                    if (lexerEngine.equalAny(DefaultKeyword.ON, Assist.END, DefaultKeyword.WHEN)) {
                        return;
                    }
                } else {

                }
            } else {
                SelectStatement selectStatement = new SelectStatement();
                mergeStatement.setUsingSelectStatement(selectStatement);
                mergeClauseParserFacade.getUsingTableReferencesClauseParser().parse(selectStatement, false);
            }
        }
    }

    private void parseMergeOnCondition(MergeStatement mergeStatement) {
        if (lexerEngine.skipIfEqualType(DefaultKeyword.ON)) {
            List<SelectItem> items = new ArrayList<>();
            if (mergeStatement.getUsingSelectStatement() != null && mergeStatement.getUsingSelectStatement().getItems() != null) {
                items.addAll(mergeStatement.getUsingSelectStatement().getItems());
            }
            mergeClauseParserFacade.getUsingWhereClauseParser().parseWhere(shardingRule, mergeStatement, items, false);
        }
    }

    private void parseMergeWhenMatch(MergeStatement mergeStatement) {
        if (lexerEngine.skipIfEqualType(DefaultKeyword.WHEN)) {
            if (lexerEngine.skipIfEqualType(OracleKeyword.MATCHED)) {
                //when matched then
                if (lexerEngine.skipIfEqualType(DefaultKeyword.THEN)) {
                    if (lexerEngine.getCurrentToken().getType() == DefaultKeyword.UPDATE) {
                        parseUpdateStatement(mergeStatement);
                    } else if (lexerEngine.getCurrentToken().getType() == DefaultKeyword.DELETE) {
                        parseDeleteStatement(mergeStatement);
                    }
                }
            } else if (lexerEngine.skipIfEqualType(DefaultKeyword.NOT)) {
                if (lexerEngine.skipIfEqualType(OracleKeyword.MATCHED)) {
                    // When not matched then
                    if (lexerEngine.skipIfEqualType(DefaultKeyword.THEN)) {
                        if (lexerEngine.getCurrentToken().getType() == DefaultKeyword.INSERT) {
                            parseInsertStatement(mergeStatement);
                        }
                    }
                }
            }

        }
    }


    private void parseTable(final MergeStatement mergeStatement) {
        //Don't support sub table on merge keyword.
//        if (lexerEngine.skipIfEqualType(Symbol.LEFT_PAREN)) {
//            mergeStatement.setSubqueryStatement(parseInternal());
//            if (lexerEngine.equalAny(DefaultKeyword.WHERE, Assist.END)) {
//                return;
//            }
//        }
        mergeClauseParserFacade.getUsingTableReferencesClauseParser().parse(mergeStatement, true);
    }

    private void parseUpdateStatement(MergeStatement mergeStatement) {
        lexerEngine.nextToken();
        lexerEngine.skipAll(updateParser.getSkippedKeywordsBetweenUpdateAndTable());
        lexerEngine.unsupportedIfEqual(updateParser.getUnsupportedKeywordsBetweenUpdateAndTable());
        UpdateStatement updateStatement = new UpdateStatement();
        Tables tables = mergeStatement.getTables();
        List<Table> tableList = tables.getTables();
        Tables updateTables = updateStatement.getTables();
        int len = tableList.size();
        for (int index = 0; index < len; index++) {
            updateTables.add(tableList.get(index));
        }
        mergeClauseParserFacade.getUpdateSetItemsClauseParser().parse(updateStatement);
        lexerEngine.skipUntil(DefaultKeyword.WHERE);
        mergeClauseParserFacade.getUpdateWhereClauseParser().parse(shardingRule, updateStatement, Collections.<SelectItem>emptyList(), false);
        mergeStatement.setUpdateStatement(updateStatement);
    }

    private void parseDeleteStatement(MergeStatement mergeStatement) {
        lexerEngine.nextToken();
        lexerEngine.skipAll(deleteParser.getSkippedKeywordsBetweenDeleteAndTable());
        lexerEngine.unsupportedIfEqual(deleteParser.getUnsupportedKeywordsBetweenDeleteAndTable());
        DeleteStatement deleteStatement = new DeleteStatement();
        lexerEngine.skipUntil(DefaultKeyword.WHERE);
        Tables tables = mergeStatement.getTables();
        List<Table> tableList = tables.getTables();
        Tables updateTables = deleteStatement.getTables();
        int len = tableList.size();
        for (int index = 0; index < len; index++) {
            updateTables.add(tableList.get(index));
        }
        mergeClauseParserFacade.getDeleteWhereClauseParser().parse(shardingRule, deleteStatement, Collections.<SelectItem>emptyList(), false);
        mergeStatement.setDeleteStatement(deleteStatement);
    }

    private void parseInsertStatement(MergeStatement mergeStatement) {
        lexerEngine.nextToken();
        InsertStatement insertStatement = new InsertStatement();
        insertStatement.setLogicSQL(sql);

        Tables tables = mergeStatement.getTables();
        List<Table> tableList = tables.getTables();
        Tables updateTables = insertStatement.getTables();
        int len = tableList.size();
        for (int index = 0; index < len; index++) {
            updateTables.add(tableList.get(index));
        }
        mergeClauseParserFacade.getInsertIntoClauseParser().parse(insertStatement);
        mergeClauseParserFacade.getInsertColumnsClauseParser().parse(insertStatement, shardingTableMetaData);
        if (lexerEngine.equalAny(DefaultKeyword.SELECT, Symbol.LEFT_PAREN)) {
            throw new UnsupportedOperationException("Cannot INSERT SELECT");
        }
        mergeClauseParserFacade.getInsertValuesClauseParser().parse(insertStatement);
        mergeClauseParserFacade.getInsertSetClauseParser().parse(insertStatement);
        mergeClauseParserFacade.getInsertDuplicateKeyUpdateClauseParser().parse(insertStatement);
        mergeStatement.setInsertStatement(insertStatement);
    }

}
