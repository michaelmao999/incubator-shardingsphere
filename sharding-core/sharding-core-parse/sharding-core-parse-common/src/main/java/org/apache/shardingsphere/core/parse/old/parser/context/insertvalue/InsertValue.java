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

package org.apache.shardingsphere.core.parse.old.parser.context.insertvalue;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLParameterMarkerExpression;

import java.util.Collection;
import java.util.List;

/**
 * Insert value.
 *
 * @author maxiaoguang
 */
@RequiredArgsConstructor
@Getter
@ToString
public final class InsertValue {
    
    private final Collection<SQLExpression> assignments;
    
    /**
     * Get parameters count.
     * 
     * @return parameters count
     */
    public int getParametersCount() {
        int result = 0;
        for (SQLExpression each : assignments) {
            if (each instanceof SQLParameterMarkerExpression) {
                result++;
            } else if (each instanceof SQLFunctionExpression) {
                result = result + countParameter((SQLFunctionExpression) each);
            }
        }
        return result;
    }

    private int countParameter(SQLFunctionExpression functionExpression) {
        int result = 0;
        List<SQLExpression> subExpressionList = functionExpression.getParameters();
        if (subExpressionList != null) {
            int len = subExpressionList.size();
            for (int index = 0; index < len; index++) {
                SQLExpression subExpress = subExpressionList.get(index);
                if (subExpress instanceof SQLParameterMarkerExpression) {
                    result++;
                } else if (subExpress instanceof SQLFunctionExpression) {
                    result = result + countParameter((SQLFunctionExpression) subExpress);
                }
            }
        }
        return result;
    }
}
