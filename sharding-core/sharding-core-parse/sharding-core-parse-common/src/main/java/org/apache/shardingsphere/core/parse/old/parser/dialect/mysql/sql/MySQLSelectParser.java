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

import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.SelectStatement;
import org.apache.shardingsphere.core.parse.old.lexer.LexerEngine;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.clause.MySQLLimitClauseParser;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.clause.MySQLSelectOptionClauseParser;
import org.apache.shardingsphere.core.parse.old.parser.dialect.mysql.clause.facade.MySQLSelectClauseParserFacade;
import org.apache.shardingsphere.core.parse.old.parser.sql.dml.select.AbstractSelectParser;
import org.apache.shardingsphere.core.rule.ShardingRule;

/**
 * Select parser for MySQL.
 *
 * @author zhangliang
 * @author panjuan
 */
public final class MySQLSelectParser extends AbstractSelectParser {
    
    private final MySQLSelectOptionClauseParser selectOptionClauseParser;
    
    private final MySQLLimitClauseParser limitClauseParser;
    
    public MySQLSelectParser(final ShardingRule shardingRule, final LexerEngine lexerEngine, final ShardingTableMetaData shardingTableMetaData) {
        super(shardingRule, lexerEngine, new MySQLSelectClauseParserFacade(shardingRule, lexerEngine), shardingTableMetaData);
        selectOptionClauseParser = new MySQLSelectOptionClauseParser(lexerEngine);
        limitClauseParser = new MySQLLimitClauseParser(lexerEngine);
    }
    
    @Override
    protected void parseInternal(final SelectStatement selectStatement, boolean isSubGroup) {
        parseSelectOption();
        parseSelectList(selectStatement, getItems());
        parseFrom(selectStatement);
        parseWhere(getShardingRule(), selectStatement, getItems(), isSubGroup);
        parseGroupBy(selectStatement);
        parseHaving();
        parseOrderBy(selectStatement);
        parseLimit(selectStatement);
        parseSelectRest();
    }
    
    private void parseSelectOption() {
        selectOptionClauseParser.parse();
    }
    
    private void parseLimit(final SelectStatement selectStatement) {
        limitClauseParser.parse(selectStatement);
    }
}
