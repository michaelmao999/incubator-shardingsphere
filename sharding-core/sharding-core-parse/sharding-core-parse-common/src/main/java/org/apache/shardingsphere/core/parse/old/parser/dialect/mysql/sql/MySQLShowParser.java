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

package org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.sql;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.parse.antlr.constant.QuoteCharacter;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dal.DALStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.token.RemoveToken;
import org.apache.shardingsphere.core.parse.antlr.sql.token.SchemaToken;
import org.apache.shardingsphere.core.parse.antlr.sql.token.TableToken;
import org.apache.shardingsphere.core.parse.old.lexer.LexerEngine;
import org.apache.shardingsphere.core.parse.old.lexer.dialect.mysql.MySQLKeyword;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;
import org.apache.shardingsphere.core.parse.old.parser.clause.TableReferencesClauseParser;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Table;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowColumnsStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowCreateTableStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowDatabasesStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowIndexStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowOtherStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowTableStatusStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowTablesStatement;
import org.apache.shardingsphere.core.parse.old.parser.sql.dal.show.AbstractShowParser;
import org.apache.shardingsphere.core.parse.util.SQLUtil;
import org.apache.shardingsphere.core.rule.ShardingRule;

/**
 * Show parser for MySQL.
 *
 * @author zhangliang
 * @author panjuan
 */
@RequiredArgsConstructor
public final class MySQLShowParser extends AbstractShowParser {
    
    private final ShardingRule shardingRule;
    
    private final LexerEngine lexerEngine;
    
    private final TableReferencesClauseParser tableReferencesClauseParser;
    
    public MySQLShowParser(final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        this.shardingRule = shardingRule;
        this.lexerEngine = lexerEngine;
        tableReferencesClauseParser = new TableReferencesClauseParser(shardingRule, lexerEngine);
    }
    
    @Override
    public DALStatement parse() {
        lexerEngine.nextToken();
        lexerEngine.skipIfEqualType(DefaultKeyword.FULL);
        if (lexerEngine.equalOne(MySQLKeyword.DATABASES)) {
            return showDatabases();
        }
        if (lexerEngine.skipIfEqual(DefaultKeyword.TABLE, MySQLKeyword.STATUS)) {
            return parseShowTableStatus();
        }
        if (lexerEngine.skipIfEqualType(MySQLKeyword.TABLES)) {
            return parseShowTables();
        }
        if (lexerEngine.skipIfEqual(MySQLKeyword.COLUMNS, MySQLKeyword.FIELDS)) {
            return parseShowColumnsFields();
        }
        if (lexerEngine.skipIfEqualType(DefaultKeyword.CREATE) && lexerEngine.skipIfEqualType(DefaultKeyword.TABLE)) {
            return parseShowCreateTable();
        }
        if (lexerEngine.skipIfEqual(DefaultKeyword.INDEX, MySQLKeyword.INDEXES, MySQLKeyword.KEYS)) {
            return parseShowIndex();
        }
        return new ShowOtherStatement();
    }
    
    private DALStatement showDatabases() {
        return new ShowDatabasesStatement();
    }
    
    private DALStatement parseShowTableStatus() {
        DALStatement result = new ShowTableStatusStatement();
        lexerEngine.nextToken();
        if (lexerEngine.equalAny(DefaultKeyword.FROM, DefaultKeyword.IN)) {
            int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length();
            lexerEngine.nextToken();
            result.addSQLToken(new RemoveToken(beginPosition, lexerEngine.getCurrentToken().getEndPosition()));
            lexerEngine.nextToken();
        }
        if (lexerEngine.skipIfEqualType(DefaultKeyword.LIKE)) {
            parseLike(result);
        }
        return result;
    }
    
    private DALStatement parseShowTables() {
        DALStatement result = new ShowTablesStatement();
        if (lexerEngine.equalAny(DefaultKeyword.FROM, DefaultKeyword.IN)) {
            int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length();
            lexerEngine.nextToken();
            result.addSQLToken(new RemoveToken(beginPosition, lexerEngine.getCurrentToken().getEndPosition()));
            lexerEngine.nextToken();
        }
        if (lexerEngine.skipIfEqualType(DefaultKeyword.LIKE)) {
            parseLike(result);
        }
        return result;
    }
    
    private DALStatement parseShowColumnsFields() {
        DALStatement result = new ShowColumnsStatement();
        lexerEngine.skipIfEqual(DefaultKeyword.FROM, DefaultKeyword.IN);
        tableReferencesClauseParser.parseSingleTableWithoutAlias(result);
        if (lexerEngine.skipIfEqual(DefaultKeyword.FROM, DefaultKeyword.IN)) {
            int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length();
            result.addSQLToken(new SchemaToken(beginPosition, lexerEngine.getCurrentToken().getEndPosition() - 1, result.getTables().getSingleTableName()));
        }
        return result;
    }
    
    private DALStatement parseShowCreateTable() {
        DALStatement result = new ShowCreateTableStatement();
        tableReferencesClauseParser.parseSingleTableWithoutAlias(result);
        return result;
    }
    
    private DALStatement parseShowIndex() {
        DALStatement result = new ShowIndexStatement();
        lexerEngine.skipIfEqual(DefaultKeyword.FROM, DefaultKeyword.IN);
        tableReferencesClauseParser.parseSingleTableWithoutAlias(result);
        if (lexerEngine.skipIfEqual(DefaultKeyword.FROM, DefaultKeyword.IN)) {
            int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length();
            result.addSQLToken(new SchemaToken(beginPosition, lexerEngine.getCurrentToken().getEndPosition() - 1, result.getTables().getSingleTableName()));
        }
        return result;
    }
    
    private void parseLike(final DALStatement dalStatement) {
        int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length() - 1;
        String literals = lexerEngine.getCurrentToken().getLiterals();
        if (shardingRule.findTableRule(literals).isPresent() || shardingRule.isBroadcastTable(literals)) {
            dalStatement.addSQLToken(new TableToken(beginPosition, literals, QuoteCharacter.getQuoteCharacter(literals), 0));
            dalStatement.getTables().add(new Table(SQLUtil.getExactlyValue(literals), null));
        }
    }
}
