package ru.pgw.ftj.queries_seim;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import ru.pgw.ftj.caches.Part;
import ru.pgw.ftj.caches.PartSupp;

public class PgwFtj_Q3 {

    // 1. Join two tables in the map phase.
    // 2. Filter out the obtained result in the reduce phase via passing a prepared predicate to the reduce phase.
    // 3. Create a dynamic cache.

    // Create cache and redistribute its partitions dynamically

    public static void calculateAverage(Ignite ignite, Set<Long> keys) {

        // get the affinity function configured for the cache
        Affinity<Long> affinityFunc = ignite.affinity("person");

        // this map stores collections of keys for each partition
        HashMap<Integer, Set<Long>> partMap = new HashMap<>();
        keys.forEach(k -> {
            int partId = affinityFunc.partition(k);

            Set<Long> keysByPartition = partMap.computeIfAbsent(partId, key -> new HashSet<Long>());
            keysByPartition.add(k);
        });

        BigDecimal total = new BigDecimal(0);

        IgniteCompute compute = ignite.compute();

        List<String> caches = Arrays.asList("person");

        // iterate over all partitions
        for (Map.Entry<Integer, Set<Long>> pair : partMap.entrySet()) {
            // send a task that gets specific keys for the partition
            //BigDecimal sum = compute.affinityCall(caches, pair.getKey().intValue(), new SumTask(pair.getValue()));
            //total = total.add(sum);
        }

        System.out.println("the average salary is " + total.floatValue() / keys.size());
    }

    public static void main(String[] args) throws Exception {
        /*try (Ignite ignite = Ignition.start(
            "/Users/arsennasibullin/dev/apache-ignite-2.11.0/config/apache-ignite/pgw-client-config.xml")) {

            IgniteCache<Long, Part> cache = ignite.getOrCreateCache("partCache");
            IgniteCache<Long, PartSupp> cacheB = ignite.getOrCreateCache("partsuppCache");

            ignite.cluster().disableWal("partCache");
            ignite.cluster().disableWal("partsuppCache");
        }*/
    }

    private static void extracted() {
        SqlFieldsQuery query = new SqlFieldsQuery(
            "SELECT * FROM PARTSUPP ps join PART p on p.p_partkey=ps.ps_partkey");
        query.setDistributedJoins(true);
        try (Ignite ignite = Ignition.start(
            "/Users/arsennasibullin/dev/apache-ignite-2.11.0/config/apache-ignite/pgw-client-config.xml")) {
            try (IgniteCache<Long, Part> cache = ignite.getOrCreateCache("partCache");
                IgniteCache<Long, PartSupp> cacheB = ignite.getOrCreateCache("partsuppCache")) {
                ignite.cluster().disableWal("partCache");
                ignite.cluster().disableWal("partsuppCache");
                // Load cache with data from the database.
                cache.loadCache(null);
                cacheB.loadCache(null);

                ignite.affinity("");

                // Execute query on cache.
                QueryCursor<List<?>> cursor = cache.query(query);
                List<List<?>> all = cursor.getAll();
                System.out.println("Size = " + all.size());
                cursor.close();
            }
        }
    }

}
