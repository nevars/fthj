package ru.pgw.ftj.queries_seim.q0.hj;

import static ru.pgw.ftj.constants.PgwConstants.PGW_HJ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.Cache.Entry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import ru.pgw.ftj.caches.PartSupp;
import ru.pgw.ftj.projections.q0.PartPartSuppProj;

/**
 * This job joins specified partitions
 */
public class JoinComputeJob extends ComputeJobAdapter {

    @TaskSessionResource
    private ComputeTaskSession session;

    @JobContextResource
    private ComputeJobContext jobCtx;

    @IgniteInstanceResource
    private Ignite ignite;

    @LoggerResource
    private IgniteLogger log;

    private final String cacheA;

    private final String cacheB;

    private final String pgwJobId;

    private final int localHashTableCapacity;

    private String nodeId;

    private String jobId;

    public JoinComputeJob(String cacheA, String cacheB, String pgwJobId, int localHashTableCapacity) {
        this.cacheA = cacheA;
        this.cacheB = cacheB;
        this.pgwJobId = pgwJobId;
        this.localHashTableCapacity = localHashTableCapacity;
    }

    @Override
    public Object execute() throws IgniteException {
        this.nodeId = ignite.cluster().localNode().id().toString();
        this.jobId = jobCtx.getJobId().toString();
        log.debug("Starting job " + jobId + " on node " + nodeId);

        IgniteCache<Object, BinaryObject> leftCache = ignite.cache(this.cacheA).withKeepBinary();
        IgniteCache<Integer, PartSupp> rightCache = ignite.cache(this.cacheB).withKeepBinary();

        // ----------- BEGIN -----------
        List<PartPartSuppProj> result = new ArrayList<>();
        int[] partitions = ignite.affinity(this.cacheA).primaryPartitions(ignite.cluster().localNode());

        int percentage_10 = partitions.length / 10;
        int numberOfProcessedPartitions = 0;
        int totalPercentageDoneWork = 0;

        for (int partitionId = 0; partitionId < partitions.length; partitionId++) {
            int partition = partitions[partitionId];

            // BUILD PHASE
            final HashMap<Integer, BinaryObject> hashTable = buildHashTable(leftCache, partition);

            if (hashTable.isEmpty()) {
                continue;
            }

            List<BinaryObject> rightCacheEntries = scanRightCacheWithPredicate(rightCache, hashTable.keySet());
            if (rightCacheEntries.isEmpty()) {
                continue;
            }

            // PROBE PHASE
            rightCacheEntries.forEach(rightCacheEntry -> {
                BinaryObject foundPartBinaryObject = hashTable.get((Integer) rightCacheEntry.field("ps_partkey"));
                if (foundPartBinaryObject != null) {
                    // found match
                    result.add(new PartPartSuppProj(foundPartBinaryObject, rightCacheEntry.field("ps_dummy")));
                }
            });
            numberOfProcessedPartitions += 1;
            if (numberOfProcessedPartitions % percentage_10 == 0) {
                totalPercentageDoneWork += 10;
                log.debug(String.format(PGW_HJ + "Node [%s] processed [%d] percentage of partitions", nodeId, totalPercentageDoneWork));
            }
        }

        return result;
    }

    private List<BinaryObject> scanRightCacheWithPredicate(
        IgniteCache<Integer, PartSupp> rightCache,
        final Set<Integer> keys) {

        final long start = System.currentTimeMillis();
        IgniteBiPredicate<Integer, BinaryObject> filter = (integer, binaryObject) ->
            keys.contains((Integer) binaryObject.field("ps_partkey"));

        List<BinaryObject> binaryObjects = rightCache.query(new ScanQuery<>(filter),
            (IgniteClosure<Entry<Integer, BinaryObject>, BinaryObject>) Entry::getValue).getAll();
        log.debug(PGW_HJ + "Fetching right cache entries took " + (System.currentTimeMillis() - start) + " ms");
        log.debug(PGW_HJ + "Job [" + jobId + "] loads " + binaryObjects.size() + " entries of " + cacheB + " cache");
        return binaryObjects;
    }

    private HashMap<Integer, BinaryObject> buildHashTable(IgniteCache<Object, BinaryObject> leftCache, int partition) {
        long start = System.currentTimeMillis();
        final HashMap<Integer, BinaryObject> hashTable = new HashMap<>(localHashTableCapacity);
        try (QueryCursor<Entry<Integer, BinaryObject>> cursor = leftCache
            .query(new ScanQuery<Integer, BinaryObject>(partition).setLocal(true))) {
            for (Cache.Entry<Integer, BinaryObject> entry : cursor) {
                BinaryObject entryValue = entry.getValue();
                hashTable.put((Integer) entryValue.field("p_partkey"), entryValue);
            }
        }
        log.debug(PGW_HJ + "Building hash table took " + (System.currentTimeMillis() - start) + " ms");
        return hashTable;
    }

}
