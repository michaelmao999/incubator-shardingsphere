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

package org.apache.shardingsphere.core.parse.antlr.sql.segment.dml.item;

import com.google.common.base.Optional;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.core.parse.antlr.constant.QuoteCharacter;
import org.apache.shardingsphere.core.parse.antlr.sql.OwnerAvailable;
import org.apache.shardingsphere.core.parse.util.SQLUtil;

/**
 * Shorthand select item segment.
 *
 * @author zhangliang
 * @author panjuan
 */
@RequiredArgsConstructor
@Getter
public final class ShorthandSelectItemSegment implements SelectItemSegment, OwnerAvailable {
    
    private final int startIndex;
    
    private int stopIndexOfOwner;
    
    private String owner;
    
    @Setter(AccessLevel.PROTECTED)
    private QuoteCharacter ownerQuoteCharacter = QuoteCharacter.NONE;
    
    @Override
    public Optional<String> getOwner() {
        return Optional.fromNullable(owner);
    }
    
    @Override
    public void setOwner(final String owner) {
        stopIndexOfOwner = startIndex + owner.length() - 1;
        this.owner = SQLUtil.getExactlyValue(owner);
        ownerQuoteCharacter = QuoteCharacter.getQuoteCharacter(owner);
    }
}
