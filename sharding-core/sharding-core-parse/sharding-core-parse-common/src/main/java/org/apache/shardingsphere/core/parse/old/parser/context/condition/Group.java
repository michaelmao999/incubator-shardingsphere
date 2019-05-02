package org.apache.shardingsphere.core.parse.old.parser.context.condition;

import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.core.parse.old.parser.clause.condition.NullCondition;

import java.util.ArrayList;
import java.util.List;


@Getter
@Setter
public class Group implements SQLCondition{
    private List<SQLCondition> expressions = new ArrayList<>();

    public Group add(SQLCondition... whereExpressions) {
        for (SQLCondition expression : whereExpressions) {
            expressions.add(expression);
        }
        return this;
    }

    public Group optimize() {
        int len = expressions.size();
        if (len == 1) {
            SQLCondition sqlCondition = expressions.get(0);
            if (sqlCondition instanceof Group) {
                this.expressions = ((Group) sqlCondition).expressions;
            } else if (sqlCondition instanceof  And || sqlCondition instanceof  Or || sqlCondition instanceof NullCondition) {
                expressions.clear();
            }
            return this;
        }
        boolean isFirstExpression = true;
        for (int index = 0; index < len; index++) {
            SQLCondition sqlCondition = expressions.get(index);
            if (isFirstExpression) {
                if (sqlCondition instanceof  And || sqlCondition instanceof  Or || sqlCondition instanceof NullCondition) {
                    expressions.remove(index);
                    index--;
                    len--;
                    continue;
                } else if (sqlCondition instanceof Group){
                    Group sbuGroup = ((Group) sqlCondition).optimize();
                    if (sbuGroup.size() == 0) {
                        expressions.remove(index);
                        index--;
                        len--;
                        continue;
                    } else if (expressions.size() == 1) {
                        expressions = sbuGroup.expressions;
                    } else if (sbuGroup.size() == 1) {
                        expressions.set(index, sbuGroup.getExpressions().get(0));
                    }

                }
                isFirstExpression = false;
            } else {
                if (sqlCondition instanceof  And || sqlCondition instanceof  Or) {
                    isFirstExpression = true;
                } else {
                    throw new IllegalArgumentException("Wrong SQL Expression (there are two condition together)");
                }
            }
        }
        SQLCondition sqlCondition = expressions.get(expressions.size() - 1);
        if (sqlCondition instanceof  And || sqlCondition instanceof  Or) {
            expressions.remove(expressions.size() - 1);
        }
        return this;
    }

    public Group add(Group group) {
        if (expressions.isEmpty()) {
            expressions.addAll(group.getExpressions());
        } else {
            expressions.add(group);
        }
        return this;
    }

    public int size() {
        return expressions.size();
    }
}
