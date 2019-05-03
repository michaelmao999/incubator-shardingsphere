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

package org.apache.shardingsphere.core.parse.antlr.sql.segment.dml.expr.complex;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.parse.antlr.sql.segment.dml.item.SelectItemSegment;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLIgnoreExpression;

/**
 * Subquery expression segment.
 * 
 * @author duhongjun
 */
@RequiredArgsConstructor
@Getter
public final class SubquerySegment implements SelectItemSegment, ComplexExpressionSegment {
    
    private final int startIndex;
    
    private final int stopIndex;

    @Override
    public SQLExpression getSQLExpression(final String sql) {
        return new SQLIgnoreExpression(sql.substring(startIndex, stopIndex + 1));
    }
}
