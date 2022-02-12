package ru.pgw.ftj.queries_seim.q14.hj;

import static ru.pgw.ftj.constants.PgwConstants.CLIENT_CONFIG;
import static ru.pgw.ftj.constants.PgwConstants.PGW_HJ;

import org.apache.commons.lang3.RandomUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import ru.pgw.ftj.caches.LineItem;
import ru.pgw.ftj.caches.Part;
import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.constants.PgwConstants;
import ru.pgw.ftj.enums.PgwClusterRole;

public class Pgw_Q14_HJ {
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start(CLIENT_CONFIG)) {

            System.out.println(PGW_HJ + "Start JOIN processing");
            // Init caches
            IgniteCache<Integer, Part> cacheA = ignite.getOrCreateCache(PgwConstants.PART_CACHE_NAME);
            IgniteCache<Integer, LineItem> cacheB = ignite.getOrCreateCache(PgwConstants.LINE_ITEM_CACHE_NAME);

            ignite.cluster().disableWal(PgwConstants.PART_CACHE_NAME);
            ignite.cluster().disableWal(PgwConstants.LINE_ITEM_CACHE_NAME);

            // Load caches
            long start = System.currentTimeMillis();
            cacheA.loadCache(null);
            System.out.println(PGW_HJ + "Cache A is loaded. It took " + (System.currentTimeMillis() - start) + " ms");
            start = System.currentTimeMillis();
            cacheB.loadCache(null);
            System.out.println(PGW_HJ + "Cache B is loaded. It took " + (System.currentTimeMillis() - start) + " ms");

            // Start JOIN processing
            final int resultListCapacity = 200_000;
            start = System.currentTimeMillis();
            Object result = null;
            try {
                result = ignite
                    .compute(ignite
                        .cluster()
                        .forAttribute(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME,
                            PgwClusterRole.WORKER.toString()))
                    .withName("task-q14-1")
                    .execute(new JoinComputeTask14("1995-09-01"), RandomUtils.nextInt());
            } catch (Throwable e) {
                System.out.println(PGW_HJ + "OOPS. LETS TRY ONCE AGAIN");
                ignite.compute().undeployTask("task-q14-1");
                result = ignite
                    .compute(ignite
                        .cluster()
                        .forAttribute(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME,
                            PgwClusterRole.WORKER.toString()))
                    .withName("task-q14-2")
                    .execute(new JoinComputeTask14("1995-09-01"), RandomUtils.nextInt());
            }
            System.out.println(PGW_HJ + "The entire join task took " + (System.currentTimeMillis() - start) + " ms");
            System.out.println(PGW_HJ + "Result is = " + result);
            cacheA.close();
            cacheB.close();
        }
    }
}
