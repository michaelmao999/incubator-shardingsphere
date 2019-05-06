package org.apache.shardingsphere.core.parse.old.parser.expression;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@Getter
public class SQLFunctionExpression implements SQLExpression {

    private final String function;

    private List<SQLExpression> parameters;

    private Object value;

    public SQLFunctionExpression(String functionName, List<SQLExpression> parameters) {
        this.function = functionName;
        this.parameters = parameters;
    }

    public SQLFunctionExpression addParameter(SQLExpression parameter) {
        if (parameters == null) {
            parameters = new ArrayList<SQLExpression>();
        }
        parameters.add(parameter);
        return this;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String toFunctionSQL() {
        if (function == null || function.length() == 0) {
            return toPureExpression();
        }
        StringBuilder builder = new StringBuilder();
        builder.append(function).append('(');
        if (parameters != null) {
            int len = parameters.size();
            for (int index = 0; index < len; index++) {
                SQLExpression sqlExpression = parameters.get(index);
                if (index != 0) {
                    builder.append(',');
                }
                if (sqlExpression instanceof SQLParameterMarkerExpression) {
                    builder.append("?");
                } else if (sqlExpression instanceof SQLTextExpression) {
                    builder.append('\'');
                    builder.append(((SQLTextExpression) sqlExpression).getText());
                    builder.append('\'');
                } else if (sqlExpression instanceof SQLIgnoreExpression) {
                    builder.append(((SQLIgnoreExpression) sqlExpression).getExpression());
                } else if (sqlExpression instanceof SQLIdentifierExpression) {
                    builder.append(((SQLIdentifierExpression) sqlExpression).getName().toLowerCase());
                } else if (sqlExpression instanceof SQLFunctionExpression) {
                    builder.append(((SQLFunctionExpression) sqlExpression).toFunctionSQL());
                } else {
                    builder.append(String.valueOf(((SQLNumberExpression) sqlExpression).getNumber()));
                }
            }
        }
        builder.append(')');
        return builder.toString();
    }

    private String toPureExpression() {
        StringBuilder builder = new StringBuilder();
        if (parameters != null) {
            int len = parameters.size();
            for (int index = 0; index < len; index++) {
                SQLExpression sqlExpression = parameters.get(index);
                if (sqlExpression instanceof SQLParameterMarkerExpression) {
                    builder.append("?");
                } else if (sqlExpression instanceof SQLTextExpression) {
                    builder.append('\'');
                    builder.append(((SQLTextExpression) sqlExpression).getText());
                    builder.append('\'');
                } else if (sqlExpression instanceof SQLIgnoreExpression) {
                    builder.append(((SQLIgnoreExpression) sqlExpression).getExpression());
                } else if (sqlExpression instanceof SQLIdentifierExpression) {
                    builder.append(((SQLIdentifierExpression) sqlExpression).getName().toLowerCase());
                } else if (sqlExpression instanceof SQLFunctionExpression) {
                    builder.append(((SQLFunctionExpression) sqlExpression).toFunctionSQL());
                } else {
                    builder.append(String.valueOf(((SQLNumberExpression) sqlExpression).getNumber()));
                }
            }
        }
        return builder.toString();
    }

    public String toString() {
        return toFunctionSQL();
    }

}
