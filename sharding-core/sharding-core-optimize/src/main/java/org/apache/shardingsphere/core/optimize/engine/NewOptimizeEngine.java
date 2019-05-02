package org.apache.shardingsphere.core.optimize.engine;

import org.apache.shardingsphere.core.strategy.route.value.GroupRouteValue;

public interface NewOptimizeEngine {
    /**
     * Optimize sharding conditions.
     *
     * @return sharding conditions
     */
    GroupRouteValue optimize();
}
