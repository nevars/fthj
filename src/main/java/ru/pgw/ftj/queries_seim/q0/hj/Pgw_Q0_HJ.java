package ru.pgw.ftj.queries_seim.q0.hj;

import static ru.pgw.ftj.constants.PgwConstants.CLIENT_CONFIG;
import static ru.pgw.ftj.constants.PgwConstants.PGW_HJ;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import ru.pgw.ftj.caches.Part;
import ru.pgw.ftj.caches.PartSupp;
import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.constants.PgwConstants;
import ru.pgw.ftj.enums.PgwClusterRole;
import ru.pgw.ftj.projections.q0.PartPartSuppProj;
import ru.pgw.ftj.util.ClusterUtils;

public class Pgw_Q0_HJ {

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start(CLIENT_CONFIG)) {

            // Init caches
            System.out.println(PGW_HJ + "Start JOIN processing");
            IgniteCache<Long, Part> cacheA = ignite.getOrCreateCache(PgwConstants.PART_CACHE_NAME);
            IgniteCache<Long, PartSupp> cacheB = ignite.getOrCreateCache(PgwConstants.PART_SUPP_CACHE_NAME);

            long start = System.currentTimeMillis();
            cacheA.loadCache(null);
            cacheB.loadCache(null);
            System.out.println(PGW_HJ + "Caches are loaded. It took " + (System.currentTimeMillis() - start) + " ms");

            ignite.cluster().disableWal(PgwConstants.PART_CACHE_NAME);
            ignite.cluster().disableWal(PgwConstants.PART_SUPP_CACHE_NAME);

            ClusterUtils.displayClusterNodePartitions(ignite, cacheA.getName(), cacheB.getName());

            final int resultListCapacity = 200_000;
            start = System.currentTimeMillis();
            List<PartPartSuppProj> result = new ArrayList<>();
            try {
                result = (List<PartPartSuppProj>) ignite
                    .compute(ignite
                        .cluster()
                        .forAttribute(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME,
                            PgwClusterRole.WORKER.toString()))
                    .execute(new JoinComputeTask(resultListCapacity), null);
            } catch (Throwable e) {
                System.out.println(PGW_HJ + "OOPS! LET'S TRY ONCE AGAIN");
                ignite.compute().localTasks().clear();
                result = (List<PartPartSuppProj>) ignite
                    .compute()
                    .execute(new JoinComputeTask(resultListCapacity), null);
            }
            System.out.println(PGW_HJ + "The entire join task took " + (System.currentTimeMillis() - start) + " ms");
            System.out.println(PGW_HJ + "Size " + result.size());
            System.out.println(PGW_HJ + "RESULT 1: " + result.get(0));
            System.out.println(PGW_HJ + "RESULT 20: " + result.get(19));
            cacheA.close();
            cacheB.close();
        }
    }

}