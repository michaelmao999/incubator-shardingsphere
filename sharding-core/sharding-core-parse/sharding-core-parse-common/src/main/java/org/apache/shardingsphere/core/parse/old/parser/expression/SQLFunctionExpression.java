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
}
