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

package org.apache.shardingsphere.core.rewrite;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.optimize.result.OptimizeResult;
import org.apache.shardingsphere.core.optimize.result.insert.InsertOptimizeResult;
import org.apache.shardingsphere.core.optimize.result.insert.InsertOptimizeResultUnit;
import org.apache.shardingsphere.core.parse.antlr.sql.Substitutable;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.token.EncryptColumnToken;
import org.apache.shardingsphere.core.parse.antlr.sql.token.InsertSetToken;
import org.apache.shardingsphere.core.parse.antlr.sql.token.InsertValuesToken;
import org.apache.shardingsphere.core.parse.antlr.sql.token.ItemsToken;
import org.apache.shardingsphere.core.parse.antlr.sql.token.RemoveToken;
import org.apache.shardingsphere.core.parse.antlr.sql.token.SQLToken;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.Condition;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLNumberExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLParameterMarkerExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLTextExpression;
import org.apache.shardingsphere.core.parse.util.SQLUtil;
import org.apache.shardingsphere.core.rewrite.placeholder.EncryptUpdateItemColumnPlaceholder;
import org.apache.shardingsphere.core.rewrite.placeholder.EncryptWhereColumnPlaceholder;
import org.apache.shardingsphere.core.rewrite.placeholder.InsertSetPlaceholder;
import org.apache.shardingsphere.core.rewrite.placeholder.InsertValuesPlaceholder;
import org.apache.shardingsphere.core.rewrite.placeholder.ShardingPlaceholder;
import org.apache.shardingsphere.core.rule.EncryptRule;
import org.apache.shardingsphere.spi.encrypt.ShardingEncryptor;
import org.apache.shardingsphere.spi.encrypt.ShardingQueryAssistedEncryptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Encrypt SQL rewrite engine.
 * 
 * <p>Rewrite logic SQL to actual SQL, should rewrite table name and optimize something.</p>
 *
 * @author panjuan
 */
public final class EncryptSQLRewriteEngine {
    
    private final EncryptRule encryptRule;
    
    private final String originalSQL;
    
    private final DatabaseType databaseType;
    
    private final SQLStatement sqlStatement;
    
    private final List<SQLToken> sqlTokens;
    
    private final List<Object> parameters;
    
    private final Map<Integer, Object> appendedIndexAndParameters;
    
    private final OptimizeResult optimizeResult;
    
    /**
     * Constructs encrypt SQL rewrite engine.
     * 
     * @param encryptRule encrypt rule
     * @param originalSQL original SQL
     * @param databaseType database type
     * @param sqlStatement SQL statement
     * @param parameters parameters
     */
    public EncryptSQLRewriteEngine(final EncryptRule encryptRule, 
                                   final String originalSQL, final DatabaseType databaseType, final SQLStatement sqlStatement, final List<Object> parameters, final OptimizeResult optimizeResult) {
        this.encryptRule = encryptRule;
        this.originalSQL = originalSQL;
        this.databaseType = databaseType;
        this.sqlStatement = sqlStatement;
        sqlTokens = sqlStatement.getSQLTokens();
        this.parameters = parameters;
        appendedIndexAndParameters = new LinkedHashMap<>();
        this.optimizeResult = optimizeResult;
    }
    
    /**
     * rewrite SQL.
     *
     * @return SQL builder
     */
    public SQLBuilder rewrite() {
        SQLBuilder result = new SQLBuilder(parameters);
        if (sqlTokens.isEmpty()) {
            return appendOriginalLiterals(result);
        }
        appendTokensAndPlaceholders(result);
        reviseParameters();
        return result;
    }
    
    private SQLBuilder appendOriginalLiterals(final SQLBuilder sqlBuilder) {
        sqlBuilder.appendLiterals(originalSQL);
        return sqlBuilder;
    }
    
    private void appendTokensAndPlaceholders(final SQLBuilder sqlBuilder) {
        int count = 0;
        sqlBuilder.appendLiterals(originalSQL.substring(0, sqlTokens.get(0).getStartIndex()));
        for (SQLToken each : sqlTokens) {
            if (each instanceof ItemsToken) {
                appendItemsToken(sqlBuilder, (ItemsToken) each, count);
            } else if (each instanceof InsertValuesToken) {
                appendInsertValuesToken(sqlBuilder, (InsertValuesToken) each, count, optimizeResult.getInsertOptimizeResult().get());
            } else if (each instanceof InsertSetToken) {
                appendInsertSetToken(sqlBuilder, (InsertSetToken) each, count, optimizeResult.getInsertOptimizeResult().get());
            } else if (each instanceof EncryptColumnToken) {
                appendEncryptColumnPlaceholder(sqlBuilder, (EncryptColumnToken) each, count);
            } else if (each instanceof RemoveToken) {
                appendRest(sqlBuilder, count, getStopIndex(each));
            }
            count++;
        }
    }
    
    private void appendItemsToken(final SQLBuilder sqlBuilder, final ItemsToken itemsToken, final int count) {
        if (!(sqlStatement instanceof InsertStatement)) {
            return;
        }
        for (int i = 0; i < itemsToken.getItems().size(); i++) {
            if (itemsToken.isFirstOfItemsSpecial() && 0 == i) {
                sqlBuilder.appendLiterals(SQLUtil.getOriginalValue(itemsToken.getItems().get(i), databaseType));
            } else {
                sqlBuilder.appendLiterals(", ");
                sqlBuilder.appendLiterals(SQLUtil.getOriginalValue(itemsToken.getItems().get(i), databaseType));
            }
        }
        appendRest(sqlBuilder, count, getStopIndex(itemsToken));
    }
    
    private void appendInsertValuesToken(final SQLBuilder sqlBuilder, final InsertValuesToken insertValuesToken, final int count, final InsertOptimizeResult insertOptimizeResult) {
        for (InsertOptimizeResultUnit each : insertOptimizeResult.getUnits()) {
            encryptInsertOptimizeResult(insertOptimizeResult.getColumnNames(), each);
        }
        sqlBuilder.appendPlaceholder(
                new InsertValuesPlaceholder(sqlStatement.getTables().getSingleTableName(), insertOptimizeResult.getColumnNames(), insertOptimizeResult.getUnits()));
        appendRest(sqlBuilder, count, getStopIndex(insertValuesToken));
    }
    
    private void appendInsertSetToken(final SQLBuilder sqlBuilder, final InsertSetToken insertSetToken, final int count, final InsertOptimizeResult insertOptimizeResult) {
        for (InsertOptimizeResultUnit each : insertOptimizeResult.getUnits()) {
            encryptInsertOptimizeResult(insertOptimizeResult.getColumnNames(), each);
        }
        sqlBuilder.appendPlaceholder(
                new InsertSetPlaceholder(sqlStatement.getTables().getSingleTableName(), insertOptimizeResult.getColumnNames(), insertOptimizeResult.getUnits()));
        appendRest(sqlBuilder, count, getStopIndex(insertSetToken));
    }
    
    private void encryptInsertOptimizeResult(final Collection<String> columnNames, final InsertOptimizeResultUnit unit) {
        for (String each : columnNames) {
            Optional<ShardingEncryptor> shardingEncryptor = encryptRule.getEncryptorEngine().getShardingEncryptor(sqlStatement.getTables().getSingleTableName(), each);
            if (shardingEncryptor.isPresent()) {
                encryptInsertOptimizeResult(unit, each, shardingEncryptor.get());
            }
        }
    }
    
    private void encryptInsertOptimizeResult(final InsertOptimizeResultUnit unit, final String columnName, final ShardingEncryptor shardingEncryptor) {
        if (shardingEncryptor instanceof ShardingQueryAssistedEncryptor) {
            String assistedColumnName = encryptRule.getEncryptorEngine().getAssistedQueryColumn(sqlStatement.getTables().getSingleTableName(), columnName).get();
            unit.setColumnValue(
                    assistedColumnName, ((ShardingQueryAssistedEncryptor) shardingEncryptor).queryAssistedEncrypt(unit.getColumnValue(columnName).toString()));
        }
        unit.setColumnValue(columnName, shardingEncryptor.encrypt(unit.getColumnValue(columnName)));
    }
    
    private void appendEncryptColumnPlaceholder(final SQLBuilder sqlBuilder, final EncryptColumnToken encryptColumnToken, final int count) {
        Optional<Condition> encryptCondition = getEncryptCondition(encryptColumnToken);
        Preconditions.checkArgument(!encryptColumnToken.isInWhere() || encryptCondition.isPresent(), "Can not find encrypt condition");
        ShardingPlaceholder result = encryptColumnToken.isInWhere() 
                ? getEncryptColumnPlaceholderFromConditions(encryptColumnToken, encryptCondition.get()) : getEncryptColumnPlaceholderFromUpdateItem(encryptColumnToken);
        sqlBuilder.appendPlaceholder(result);
        appendRest(sqlBuilder, count, getStopIndex(encryptColumnToken));
    }
    
    private Optional<Condition> getEncryptCondition(final EncryptColumnToken encryptColumnToken) {
        List<Condition> conditions = sqlStatement.getEncryptConditions().getOrCondition().findConditions(encryptColumnToken.getColumn());
        if (conditions.isEmpty()) {
            return Optional.absent();
        }
        if (1 == conditions.size()) {
            return Optional.of(conditions.iterator().next());
        }
        return Optional.of(conditions.get(getEncryptConditionIndex(encryptColumnToken)));
    }
    
    private int getEncryptConditionIndex(final EncryptColumnToken encryptColumnToken) {
        List<SQLToken> result = new ArrayList<>(Collections2.filter(sqlTokens, new Predicate<SQLToken>() {
            
            @Override
            public boolean apply(final SQLToken input) {
                return input instanceof EncryptColumnToken && ((EncryptColumnToken) input).getColumn().equals(encryptColumnToken.getColumn());
            }
        }));
        return result.indexOf(encryptColumnToken);
    }
    
    private EncryptWhereColumnPlaceholder getEncryptColumnPlaceholderFromConditions(final EncryptColumnToken encryptColumnToken, final Condition encryptCondition) {
        List<Comparable<?>> encryptColumnValues = getFinalEncryptColumnValues(encryptColumnToken, encryptCondition.getConditionValues(parameters));
        encryptParameters(encryptCondition.getPositionIndexMap(), encryptColumnValues);
        return new EncryptWhereColumnPlaceholder(encryptColumnToken.getColumn().getTableName(), getFinalEncryptColumnName(encryptColumnToken),
                getPositionValues(encryptCondition.getPositionValueMap().keySet(), encryptColumnValues), encryptCondition.getPositionIndexMap().keySet(), encryptCondition.getOperator());
    }
    
    private List<Comparable<?>> getFinalEncryptColumnValues(final EncryptColumnToken encryptColumnToken, final List<Comparable<?>> originalColumnValues) {
        ShardingEncryptor shardingEncryptor = getShardingEncryptor(encryptColumnToken);
        return shardingEncryptor instanceof ShardingQueryAssistedEncryptor
                ? getEncryptAssistedColumnValues((ShardingQueryAssistedEncryptor) shardingEncryptor, originalColumnValues) : getEncryptColumnValues(shardingEncryptor, originalColumnValues);
    }
    
    private ShardingEncryptor getShardingEncryptor(final EncryptColumnToken encryptColumnToken) {
        return encryptRule.getEncryptorEngine().getShardingEncryptor(encryptColumnToken.getColumn().getTableName(), encryptColumnToken.getColumn().getName()).get();
    }
    
    private List<Comparable<?>> getEncryptAssistedColumnValues(final ShardingQueryAssistedEncryptor shardingEncryptor, final List<Comparable<?>> originalColumnValues) {
        return Lists.transform(originalColumnValues, new Function<Comparable<?>, Comparable<?>>() {
            
            @Override
            public Comparable<?> apply(final Comparable<?> input) {
                return shardingEncryptor.queryAssistedEncrypt(input.toString());
            }
        });
    }
    
    private List<Comparable<?>> getEncryptColumnValues(final ShardingEncryptor shardingEncryptor, final List<Comparable<?>> originalColumnValues) {
        return Lists.transform(originalColumnValues, new Function<Comparable<?>, Comparable<?>>() {
            
            @Override
            public Comparable<?> apply(final Comparable<?> input) {
                return String.valueOf(shardingEncryptor.encrypt(input.toString()));
            }
        });
    }
    
    private void encryptParameters(final Map<Integer, Integer> positionIndexes, final List<Comparable<?>> encryptColumnValues) {
        if (!positionIndexes.isEmpty()) {
            for (Entry<Integer, Integer> entry : positionIndexes.entrySet()) {
                parameters.set(entry.getValue(), encryptColumnValues.get(entry.getKey()));
            }
        }
    }
    
    private String getFinalEncryptColumnName(final EncryptColumnToken encryptColumnToken) {
        return getShardingEncryptor(encryptColumnToken) instanceof ShardingQueryAssistedEncryptor ? getEncryptAssistedColumnName(encryptColumnToken) : encryptColumnToken.getColumn().getName();
    }
    
    private Map<Integer, Comparable<?>> getPositionValues(final Collection<Integer> valuePositions, final List<Comparable<?>> encryptColumnValues) {
        Map<Integer, Comparable<?>> result = new LinkedHashMap<>();
        for (int each : valuePositions) {
            result.put(each, encryptColumnValues.get(each));
        }
        return result;
    }
    
    private EncryptUpdateItemColumnPlaceholder getEncryptColumnPlaceholderFromUpdateItem(final EncryptColumnToken encryptColumnToken) {
        ShardingEncryptor shardingEncryptor = getShardingEncryptor(encryptColumnToken);
        List<Comparable<?>> originalColumnValues = getOriginalColumnValuesFromUpdateItem(encryptColumnToken);
        List<Comparable<?>> encryptColumnValues = getEncryptColumnValues(shardingEncryptor, originalColumnValues);
        List<Comparable<?>> encryptAssistedColumnValues = shardingEncryptor instanceof ShardingQueryAssistedEncryptor 
                ? getEncryptAssistedColumnValues((ShardingQueryAssistedEncryptor) shardingEncryptor, originalColumnValues) : new LinkedList<Comparable<?>>();
        encryptParameters(getPositionIndexesFromUpdateItem(encryptColumnToken), encryptColumnValues);
        appendIndexAndParameters(encryptColumnToken, encryptAssistedColumnValues);
        return shardingEncryptor instanceof ShardingQueryAssistedEncryptor ? getEncryptUpdateItemColumnPlaceholder(encryptColumnToken, encryptColumnValues, encryptAssistedColumnValues) 
                : getEncryptUpdateItemColumnPlaceholder(encryptColumnToken, encryptColumnValues);
    }
    
    private List<Comparable<?>> getOriginalColumnValuesFromUpdateItem(final EncryptColumnToken encryptColumnToken) {
        List<Comparable<?>> result = new LinkedList<>();
        SQLExpression sqlExpression = ((UpdateStatement) sqlStatement).getAssignments().get(encryptColumnToken.getColumn());
        if (sqlExpression instanceof SQLParameterMarkerExpression) {
            result.add(parameters.get(((SQLParameterMarkerExpression) sqlExpression).getIndex()).toString());
        } else if (sqlExpression instanceof SQLTextExpression) {
            result.add(((SQLTextExpression) sqlExpression).getText());
        } else if (sqlExpression instanceof SQLNumberExpression) {
            result.add((Comparable) ((SQLNumberExpression) sqlExpression).getNumber());
        }
        return result;
    }
    
    private Map<Integer, Integer> getPositionIndexesFromUpdateItem(final EncryptColumnToken encryptColumnToken) {
        SQLExpression result = ((UpdateStatement) sqlStatement).getAssignments().get(encryptColumnToken.getColumn());
        if (result instanceof SQLParameterMarkerExpression) {
            return Collections.singletonMap(0, ((SQLParameterMarkerExpression) result).getIndex());
        }
        return new LinkedHashMap<>();
    }
    
    private void appendIndexAndParameters(final EncryptColumnToken encryptColumnToken, final List<Comparable<?>> encryptAssistedColumnValues) {
        if (encryptAssistedColumnValues.isEmpty()) {
            return;
        }
        if (!isUsingParameters(encryptColumnToken)) {
            return;
        }
        appendedIndexAndParameters.put(getEncryptAssistedParameterIndex(encryptColumnToken), encryptAssistedColumnValues.get(0));
    }
    
    private boolean isUsingParameters(final EncryptColumnToken encryptColumnToken) {
        return ((UpdateStatement) sqlStatement).getAssignments().get(encryptColumnToken.getColumn()) instanceof SQLParameterMarkerExpression;
    }
    
    private int getEncryptAssistedParameterIndex(final EncryptColumnToken encryptColumnToken) {
        return getPositionIndexesFromUpdateItem(encryptColumnToken).values().iterator().next() + 1;
    }
    
    private EncryptUpdateItemColumnPlaceholder getEncryptUpdateItemColumnPlaceholder(final EncryptColumnToken encryptColumnToken, final List<Comparable<?>> encryptColumnValues) {
        if (isUsingParameters(encryptColumnToken)) {
            return new EncryptUpdateItemColumnPlaceholder(encryptColumnToken.getColumn().getTableName(), encryptColumnToken.getColumn().getName());
        }
        return new EncryptUpdateItemColumnPlaceholder(encryptColumnToken.getColumn().getTableName(), encryptColumnToken.getColumn().getName(),
                getPositionValues(Collections.singletonList(0), encryptColumnValues).values().iterator().next());
    }
    
    private EncryptUpdateItemColumnPlaceholder getEncryptUpdateItemColumnPlaceholder(final EncryptColumnToken encryptColumnToken,
                                                                                     final List<Comparable<?>> encryptColumnValues, final List<Comparable<?>> encryptAssistedColumnValues) {
        if (isUsingParameters(encryptColumnToken)) {
            return new EncryptUpdateItemColumnPlaceholder(encryptColumnToken.getColumn().getTableName(), encryptColumnToken.getColumn().getName(), getEncryptAssistedColumnName(encryptColumnToken));
        }
        return new EncryptUpdateItemColumnPlaceholder(encryptColumnToken.getColumn().getTableName(), encryptColumnToken.getColumn().getName(),
                getPositionValues(Collections.singletonList(0), encryptColumnValues).values().iterator().next(), getEncryptAssistedColumnName(encryptColumnToken), encryptAssistedColumnValues.get(0));
    }
    
    private String getEncryptAssistedColumnName(final EncryptColumnToken encryptColumnToken) {
        Column column = encryptColumnToken.getColumn();
        Optional<String> result = encryptRule.getEncryptorEngine().getAssistedQueryColumn(column.getTableName(), column.getName());
        Preconditions.checkArgument(result.isPresent(), "Can not find the assistedColumn of %s", encryptColumnToken.getColumn().getName());
        return result.get();
    }
    
    private int getStopIndex(final SQLToken sqlToken) {
        return sqlToken instanceof Substitutable ? ((Substitutable) sqlToken).getStopIndex() + 1 : sqlToken.getStartIndex();
    }
    
    private void appendRest(final SQLBuilder sqlBuilder, final int count, final int startIndex) {
        int stopPosition = sqlTokens.size() - 1 == count ? originalSQL.length() : sqlTokens.get(count + 1).getStartIndex();
        sqlBuilder.appendLiterals(originalSQL.substring(startIndex, stopPosition));
    }
    
    private void reviseParameters() {
        for (Entry<Integer, Object> entry : appendedIndexAndParameters.entrySet()) {
            parameters.add(entry.getKey(), entry.getValue());
        }
    }
}
