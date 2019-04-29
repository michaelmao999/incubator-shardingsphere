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

package org.apache.shardingsphere.core.parse;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dal.DALStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dal.SetStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dcl.DCLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.ddl.DDLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DMLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DQLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DeleteStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.SelectStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.tcl.TCLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.token.SchemaToken;
import org.apache.shardingsphere.core.parse.old.lexer.LexerEngine;
import org.apache.shardingsphere.core.parse.old.lexer.LexerEngineFactory;
import org.apache.shardingsphere.core.parse.old.lexer.dialect.mysql.MySQLKeyword;
import org.apache.shardingsphere.core.parse.old.lexer.token.Assist;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;
import org.apache.shardingsphere.core.parse.old.lexer.token.Keyword;
import org.apache.shardingsphere.core.parse.old.lexer.token.Symbol;
import org.apache.shardingsphere.core.parse.old.lexer.token.TokenType;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.DescribeStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowColumnsStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowCreateTableStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowDatabasesStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowIndexStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowOtherStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowTableStatusStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.ShowTablesStatement;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.statement.UseStatement;
import org.apache.shardingsphere.core.parse.old.parser.exception.SQLParsingException;

/**
 * SQL judge engine.
 *
 * @author zhangliang
 * @author panjuan
 */
@RequiredArgsConstructor
public final class SQLJudgeEngine {
    
    private final String sql;
    
    /**
     * Judge SQL type only.
     *
     * @return SQL statement
     */
    public SQLStatement judge() {
        LexerEngine lexerEngine = LexerEngineFactory.newInstance(DatabaseType.MySQL, sql);
        lexerEngine.nextToken();
        while (true) {
            TokenType tokenType = lexerEngine.getCurrentToken().getType();
            if (tokenType instanceof Keyword) {
                if (DQLStatement.isDQL(tokenType)) {
                    return getDQLStatement();
                }
                if (DMLStatement.isDML(tokenType)) {
                    return getDMLStatement(tokenType);
                }
                if (TCLStatement.isTCL(tokenType)) {
                    return getTCLStatement();
                }
                if (DALStatement.isDAL(tokenType)) {
                    return getDALStatement(tokenType, lexerEngine);
                }
                lexerEngine.nextToken();
                TokenType secondaryTokenType = lexerEngine.getCurrentToken().getType();
                if (DDLStatement.isDDL(tokenType, secondaryTokenType)) {
                    return getDDLStatement();
                }
                if (DCLStatement.isDCL(tokenType, secondaryTokenType)) {
                    return getDCLStatement();
                }
                if (TCLStatement.isTCLUnsafe(DatabaseType.MySQL, tokenType, lexerEngine)) {
                    return getTCLStatement();
                }
                if (DefaultKeyword.SET.equals(tokenType)) {
                    return new SetStatement();
                }
            } else {
                lexerEngine.nextToken();
            }
            if (sql.toUpperCase().startsWith("CALL")) {
                return getDQLStatement();
            }
            if (tokenType instanceof Assist && Assist.END == tokenType) {
                throw new SQLParsingException("Unsupported SQL statement: [%s]", sql);
            }
        }
    }
    
    private SQLStatement getDQLStatement() {
        return new SelectStatement();
    }
    
    private SQLStatement getDMLStatement(final TokenType tokenType) {
        if (DefaultKeyword.INSERT == tokenType) {
            return new InsertStatement();
        }
        if (DefaultKeyword.UPDATE == tokenType) {
            return new UpdateStatement();
        }
        return new DeleteStatement();
    }
    
    private SQLStatement getDDLStatement() {
        return new DDLStatement();
    }
    
    private SQLStatement getDCLStatement() {
        return new DCLStatement();
    }
    
    private SQLStatement getTCLStatement() {
        return new TCLStatement();
    }
    
    private SQLStatement getDALStatement(final TokenType tokenType, final LexerEngine lexerEngine) {
        if (DefaultKeyword.USE == tokenType) {
            lexerEngine.nextToken();
            return new UseStatement(lexerEngine.getCurrentToken().getLiterals());
        }
        if (DefaultKeyword.DESC == tokenType || MySQLKeyword.DESCRIBE == tokenType) {
            return new DescribeStatement();
        }
        return getShowStatement(lexerEngine);
    }
    
    private SQLStatement getShowStatement(final LexerEngine lexerEngine) {
        lexerEngine.nextToken();
        lexerEngine.skipIfEqualType(DefaultKeyword.FULL);
        if (lexerEngine.equalOne(MySQLKeyword.DATABASES)) {
            return new ShowDatabasesStatement();
        }
        if (lexerEngine.skipIfEqual(DefaultKeyword.TABLE, MySQLKeyword.STATUS)) {
            return parseShowTableStatus(lexerEngine);
        }
        if (lexerEngine.skipIfEqualType(MySQLKeyword.TABLES)) {
            return parseShowTables(lexerEngine);
        }
        if (lexerEngine.skipIfEqual(MySQLKeyword.COLUMNS, MySQLKeyword.FIELDS)) {
            return parseShowColumnsFields(lexerEngine);
        }
        if (lexerEngine.skipIfEqualType(DefaultKeyword.CREATE) && lexerEngine.skipIfEqualType(DefaultKeyword.TABLE)) {
            return parseShowCreateTable(lexerEngine);
        }
        if (lexerEngine.skipIfEqual(DefaultKeyword.INDEX, MySQLKeyword.INDEXES, MySQLKeyword.KEYS)) {
            return parseShowIndex(lexerEngine);
        }
        return new ShowOtherStatement();
    }
    
    private DALStatement parseShowTableStatus(final LexerEngine lexerEngine) {
        DALStatement result = new ShowTableStatusStatement();
        lexerEngine.nextToken();
        if (lexerEngine.skipIfEqual(DefaultKeyword.FROM, DefaultKeyword.IN)) {
            int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length();
            result.addSQLToken(new SchemaToken(beginPosition, lexerEngine.getCurrentToken().getEndPosition() - 1, null));
        }
        return result;
    }
    
    private DALStatement parseShowTables(final LexerEngine lexerEngine) {
        DALStatement result = new ShowTablesStatement();
        if (lexerEngine.skipIfEqual(DefaultKeyword.FROM, DefaultKeyword.IN)) {
            int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length();
            result.addSQLToken(new SchemaToken(beginPosition, lexerEngine.getCurrentToken().getEndPosition() - 1, null));
        }
        return result;
    }
    
    private DALStatement parseShowColumnsFields(final LexerEngine lexerEngine) {
        DALStatement result = new ShowColumnsStatement();
        lexerEngine.skipIfEqual(DefaultKeyword.FROM, DefaultKeyword.IN);
        parseSingleTableWithSchema(lexerEngine, result);
        if (lexerEngine.skipIfEqual(DefaultKeyword.FROM, DefaultKeyword.IN)) {
            int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length();
            result.addSQLToken(new SchemaToken(beginPosition, lexerEngine.getCurrentToken().getEndPosition() - 1, null));
        }
        return result;
    }
    
    private DALStatement parseShowCreateTable(final LexerEngine lexerEngine) {
        DALStatement result = new ShowCreateTableStatement();
        parseSingleTableWithSchema(lexerEngine, result);
        return result;
    }
    
    private DALStatement parseShowIndex(final LexerEngine lexerEngine) {
        DALStatement result = new ShowIndexStatement();
        lexerEngine.skipIfEqual(DefaultKeyword.FROM, DefaultKeyword.IN);
        parseSingleTableWithSchema(lexerEngine, result);
        if (lexerEngine.skipIfEqual(DefaultKeyword.FROM, DefaultKeyword.IN)) {
            int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length();
            result.addSQLToken(new SchemaToken(beginPosition, lexerEngine.getCurrentToken().getEndPosition() - 1, null));
        }
        return result;
    }
    
    private void parseSingleTableWithSchema(final LexerEngine lexerEngine, final SQLStatement sqlStatement) {
        int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length();
        lexerEngine.nextToken();
        if (lexerEngine.skipIfEqualType(Symbol.DOT)) {
            sqlStatement.addSQLToken(new SchemaToken(beginPosition, lexerEngine.getCurrentToken().getEndPosition() - 1, null));
            lexerEngine.nextToken();
        }
    }
}
