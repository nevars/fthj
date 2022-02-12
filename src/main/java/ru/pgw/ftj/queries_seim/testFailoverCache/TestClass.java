package ru.pgw.ftj.queries_seim.testFailoverCache;

import static ru.pgw.ftj.constants.PgwConstants.CLIENT_CONFIG;
import static ru.pgw.ftj.constants.PgwConstants.PGW_FTJ;

import java.io.Serializable;
import java.util.List;
import javax.cache.Cache.Entry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.constants.PgwConstants;
import ru.pgw.ftj.enums.PgwClusterRole;

public class TestClass {

    public static void main(String[] args) throws Exception {

        try (Ignite ignite = Ignition.start(CLIENT_CONFIG)) {

            System.out.println(PGW_FTJ + "Start TEST processing");
            // Init caches
            IgniteCache<Long, BinaryObject> cacheA = ignite.getOrCreateCache(PgwConstants.PART_CACHE_NAME)
                .withKeepBinary();

            CacheConfiguration cacheConfiguration = new CacheConfiguration(FaultTolerantConstants.FAILOVER_CACHE);
            cacheConfiguration.setNodeFilter((IgnitePredicate<ClusterNode>) clusterNode ->
                clusterNode.attributes().containsKey(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME) &&
                    PgwClusterRole.DATA_KEEPER.toString()
                        .equals(clusterNode.attributes().get(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME)));
            ignite.addCacheConfiguration(cacheConfiguration);
            IgniteCache<Object, List<? extends Serializable>> failoverCache = ignite.getOrCreateCache(
                cacheConfiguration);

            long start = System.currentTimeMillis();
            cacheA.loadCache(null);
            System.out.println(PGW_FTJ + "Caches are loaded. It took " + (System.currentTimeMillis() - start) + " ms");

            ignite.cluster().disableWal(PgwConstants.PART_CACHE_NAME);

            start = System.currentTimeMillis();
            ComputeTaskFuture<Object> objectComputeTaskFuture = ignite
                .compute(ignite
                    .cluster()
                    .forAttribute(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME,
                        PgwClusterRole.WORKER.toString()))
                .executeAsync(new TestTask(), null);

            List<Entry<Object, Object>> all = ignite.getOrCreateCache(FaultTolerantConstants.FAILOVER_CACHE)
                .query(new ScanQuery<>()).getAll();
            System.out.println(PGW_FTJ + "The entire join task took " + (System.currentTimeMillis() - start) + " ms");
            cacheA.close();
            failoverCache.close();
        }
    }

}
