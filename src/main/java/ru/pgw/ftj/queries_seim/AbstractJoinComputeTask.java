package ru.pgw.ftj.queries_seim;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.enums.PgwClusterRole;

public abstract class AbstractJoinComputeTask extends ComputeTaskAdapter<Object, Object> {

    @IgniteInstanceResource
    protected Ignite ignite;

    @LoggerResource
    protected IgniteLogger log;

    protected String logPrefix;

    protected IgniteFuture<Object> recoverJobData(String taskJobIdSuffix) {
        log.debug(logPrefix + "START LOADING DATA FROM RECOVERY CACHE");
        return ignite.compute(ignite
                .cluster()
                .forAttribute(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME,
                    PgwClusterRole.DATA_KEEPER.toString()))
            .applyAsync((IgniteClosure<Ignite, Object>) ignite -> {
                IgniteBiPredicate<String, Object> filter = (s, value) ->
                    s.startsWith(taskJobIdSuffix);
                return ignite.cache(FaultTolerantConstants.FAILOVER_CACHE)
                    .query(new ScanQuery<>(filter))
                    .getAll();
            }, ignite);
    }

    protected void initFailoverCache() {
        CacheConfiguration cacheConfiguration = new CacheConfiguration(FaultTolerantConstants.FAILOVER_CACHE);
        cacheConfiguration.setNodeFilter((IgnitePredicate<ClusterNode>) clusterNode ->
            clusterNode.attributes().containsKey(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME) &&
                PgwClusterRole.DATA_KEEPER.toString()
                    .equals(clusterNode.attributes().get(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME)));
        ignite.addCacheConfiguration(cacheConfiguration);
    }
}
