package ru.pgw.ftj.queries_seim.q0.ftj;

import static ru.pgw.ftj.constants.PgwConstants.CLIENT_CONFIG;
import static ru.pgw.ftj.constants.PgwConstants.PGW_FTJ;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import ru.pgw.ftj.caches.Part;
import ru.pgw.ftj.caches.PartSupp;
import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.constants.PgwConstants;
import ru.pgw.ftj.enums.PgwClusterRole;
import ru.pgw.ftj.projections.q0.PartPartSuppProj;

public class Pgw_Q0_FTJ {

    public static void main(String[] args) {

        try (Ignite ignite = Ignition.start(CLIENT_CONFIG)) {

            System.out.println(PGW_FTJ + "Start JOIN processing");
            // Init caches
            initFailoverCache(ignite);
            IgniteCache<Integer, Part> cacheA = ignite.getOrCreateCache(PgwConstants.PART_CACHE_NAME);
            IgniteCache<Integer, PartSupp> cacheB = ignite.getOrCreateCache(PgwConstants.PART_SUPP_CACHE_NAME);

            ignite.cluster().disableWal(PgwConstants.PART_CACHE_NAME);
            ignite.cluster().disableWal(PgwConstants.PART_SUPP_CACHE_NAME);

            // Load caches
            long start = System.currentTimeMillis();
            cacheA.loadCache(null);
            System.out.println(PGW_FTJ + "Cache A is loaded. It took " + (System.currentTimeMillis() - start) + " ms");
            start = System.currentTimeMillis();
            cacheB.loadCache(null);
            System.out.println(PGW_FTJ + "Cache B is loaded. It took " + (System.currentTimeMillis() - start) + " ms");

            // Start JOIN processing
            final int resultListCapacity = 200_000;
            start = System.currentTimeMillis();
            List<PartPartSuppProj> result = (List<PartPartSuppProj>) ignite
                .compute(ignite
                    .cluster()
                    .forAttribute(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME,
                        PgwClusterRole.WORKER.toString()))
                .execute(new FtjJoinComputeTask(resultListCapacity), null);
            System.out.println(PGW_FTJ + "The entire join task took " + (System.currentTimeMillis() - start) + " ms");
            System.out.println(PGW_FTJ + "Size " + result.size());
            cacheA.close();
            cacheB.close();

            IgniteCache<Object, Object> failoverCache = ignite.getOrCreateCache(FaultTolerantConstants.FAILOVER_CACHE);
            int sizeOfFailoverCache = failoverCache
                .query(new ScanQuery<>())
                .getAll()
                .size();
            System.out.println(PGW_FTJ + "The size of failover cache = " + sizeOfFailoverCache);
            failoverCache.close();
        }
    }

    private static void initFailoverCache(Ignite ignite) {
        CacheConfiguration cacheConfiguration = new CacheConfiguration(FaultTolerantConstants.FAILOVER_CACHE);
        cacheConfiguration.setNodeFilter((IgnitePredicate<ClusterNode>) clusterNode ->
            clusterNode.attributes().containsKey(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME) &&
                PgwClusterRole.DATA_KEEPER.toString()
                    .equals(clusterNode.attributes().get(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME)));
        ignite.addCacheConfiguration(cacheConfiguration);
        ignite.getOrCreateCache(FaultTolerantConstants.FAILOVER_CACHE).loadCache(null);
    }

}
