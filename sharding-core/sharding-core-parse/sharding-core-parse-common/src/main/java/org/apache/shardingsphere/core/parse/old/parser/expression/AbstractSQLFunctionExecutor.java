package org.apache.shardingsphere.core.parse.old.parser.expression;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;

@RequiredArgsConstructor
@Getter(AccessLevel.PROTECTED)
public abstract  class AbstractSQLFunctionExecutor implements SQLFunctionExector {
    private final ShardingTableMetaData shardingTableMetaData;

}
