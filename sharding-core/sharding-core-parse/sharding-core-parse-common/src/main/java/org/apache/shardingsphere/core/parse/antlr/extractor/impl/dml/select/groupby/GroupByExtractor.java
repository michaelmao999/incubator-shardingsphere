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

package org.apache.shardingsphere.core.parse.antlr.extractor.impl.dml.select.groupby;

import com.google.common.base.Optional;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.shardingsphere.core.parse.antlr.extractor.api.OptionalSQLSegmentExtractor;
import org.apache.shardingsphere.core.parse.antlr.extractor.impl.dml.select.orderby.OrderByItemExtractor;
import org.apache.shardingsphere.core.parse.antlr.extractor.util.ExtractorUtils;
import org.apache.shardingsphere.core.parse.antlr.extractor.util.RuleName;
import org.apache.shardingsphere.core.parse.antlr.sql.segment.dml.order.GroupBySegment;

import java.util.Map;

/**
 * Group by extractor.
 *
 * @author duhongjun
 * @author panjuan
 * @author zhangliang
 */
@RequiredArgsConstructor
public abstract class GroupByExtractor implements OptionalSQLSegmentExtractor {
    
    private final OrderByItemExtractor orderByItemExtractor;
    
    @Override
    public final Optional<GroupBySegment> extract(final ParserRuleContext ancestorNode, final Map<ParserRuleContext, Integer> parameterMarkerIndexes) {
        Optional<ParserRuleContext> groupByNode = ExtractorUtils.findFirstChildNode(findMainQueryNode(ancestorNode, false), RuleName.GROUP_BY_CLAUSE);
        return groupByNode.isPresent() ? Optional.of(new GroupBySegment(groupByNode.get().getStop().getStopIndex(), orderByItemExtractor.extract(groupByNode.get(), parameterMarkerIndexes)))
                : Optional.<GroupBySegment>absent();
    }
    
    private ParserRuleContext findMainQueryNode(final ParserRuleContext ancestorNode, final boolean isFromRecursive) {
        Optional<ParserRuleContext> tableReferencesNode = ExtractorUtils.findFirstChildNode(ancestorNode, RuleName.TABLE_REFERENCES);
        if (!tableReferencesNode.isPresent()) {
            return ancestorNode;
        }
        ParserRuleContext result = tableReferencesNode.get();
        Optional<ParserRuleContext> subqueryNode = ExtractorUtils.findSingleNodeFromFirstDescendant(tableReferencesNode.get(), RuleName.SUBQUERY);
        boolean isFromRecursiveInMethod = false;
        if (subqueryNode.isPresent()) {
            isFromRecursiveInMethod = true;
            result = findMainQueryNode(subqueryNode.get(), true);
        }
        return isFromRecursive || isFromRecursiveInMethod ? result : ancestorNode;
    }
}
