package ru.pgw.ftj.queries_seim.q14.ftj;

import static ru.pgw.ftj.constants.PgwConstants.CLIENT_CONFIG;
import static ru.pgw.ftj.constants.PgwConstants.PGW_FTJ;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import ru.pgw.ftj.caches.LineItem;
import ru.pgw.ftj.caches.Part;
import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.constants.PgwConstants;
import ru.pgw.ftj.enums.PgwClusterRole;

public class Pgw_Q14_FTJ {

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start(CLIENT_CONFIG)) {

            System.out.println(PGW_FTJ + "Start JOIN processing");
            // Init caches
            IgniteCache<Integer, Part> cacheA = ignite.getOrCreateCache(PgwConstants.PART_CACHE_NAME);
            IgniteCache<Integer, LineItem> cacheB = ignite.getOrCreateCache(PgwConstants.LINE_ITEM_CACHE_NAME);

            ignite.cluster().disableWal(PgwConstants.PART_CACHE_NAME);
            ignite.cluster().disableWal(PgwConstants.LINE_ITEM_CACHE_NAME);

            // Load caches
            long start = System.currentTimeMillis();
            cacheA.loadCache(null);
            System.out.println(PGW_FTJ + "Cache A is loaded. It took " + (System.currentTimeMillis() - start) + " ms");
            start = System.currentTimeMillis();
            cacheB.loadCache(null);
            System.out.println(PGW_FTJ + "Cache B is loaded. It took " + (System.currentTimeMillis() - start) + " ms");

            // Start JOIN processing
            start = System.currentTimeMillis();
            Object result = ignite
                .compute(ignite
                    .cluster()
                    .forAttribute(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME,
                        PgwClusterRole.WORKER.toString()))
                .execute(new FtjJoinComputeTask14("1995-09-01"), null);
            System.out.println(PGW_FTJ + "The entire join task took " + (System.currentTimeMillis() - start) + " ms");
            System.out.println(PGW_FTJ + "Result is = " + result);
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
}
