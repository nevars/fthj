package ru.pgw.ftj.queries_seim.q0.ftj;

import static ru.pgw.ftj.constants.PgwConstants.PGW_FTJ;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
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
import org.apache.ignite.compute.ComputeJobMasterLeaveAware;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import ru.pgw.ftj.caches.PartSupp;
import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.constants.PgwConstants;
import ru.pgw.ftj.projections.q0.PartPartSuppProj;
import ru.pgw.ftj.util.FaultTolerantUtils;
import ru.pgw.ftj.util.JobUtils;

/**
 * This job joins specified partitions
 */
public class FtjJoinComputeJob extends ComputeJobAdapter implements ComputeJobMasterLeaveAware {

    @TaskSessionResource
    private ComputeTaskSession taskSession;

    @JobContextResource
    private ComputeJobContext jobCtx;

    @IgniteInstanceResource
    private Ignite ignite;

    @LoggerResource
    private IgniteLogger log;

    private final String cacheNameA;

    private final String cacheNameB;

    private final int localHashTableCapacity;

    private String nodeId;

    private String jobId;

    private String taskId;

    private boolean isRecoveryJob = false;

    private IgniteCache<Object, BinaryObject> leftCache;

    private IgniteCache<Integer, PartSupp> rightCache;

    private IgniteCache<Object, List<? extends Serializable>> failoverCache;

    public FtjJoinComputeJob(String cacheNameA, String cacheNameB, int localHashTableCapacity) {
        this.cacheNameA = cacheNameA;
        this.cacheNameB = cacheNameB;
        this.localHashTableCapacity = localHashTableCapacity;
    }

    private void initJobResources() {
        this.nodeId = ignite.cluster().localNode().id().toString();
        this.jobId = jobCtx.getJobId().toString();
        this.taskId = taskSession.getId().toString();
        this.leftCache = ignite.cache(this.cacheNameA).withKeepBinary();
        this.rightCache = ignite.cache(this.cacheNameB).withKeepBinary();
        this.failoverCache = ignite.getOrCreateCache(FaultTolerantConstants.FAILOVER_CACHE);
        log.debug(PGW_FTJ + "Starting job " + jobId + " on node " + nodeId);
        jobCtx.setAttribute(FaultTolerantConstants.TASK_JOB_ID_ATTRIBUTE_NAME, taskId + PgwConstants.DELIMETER + jobId);

        if (jobCtx.getAttributes().containsKey(FaultTolerantConstants.TIME_TO_RECOVER_JOD_ATTRIBUTE_NAME)) {
            long start = jobCtx.getAttribute(FaultTolerantConstants.TIME_TO_RECOVER_JOD_ATTRIBUTE_NAME);
            log.debug(PGW_FTJ + "Time to recover job took " + (System.currentTimeMillis() - start) + " ms");
        }
    }

    @Override
    public Object execute() throws IgniteException {
        initJobResources();

        // ----------- BEGIN -----------
        List<PartPartSuppProj> result = new ArrayList<>();
        int[] partitions = getPartitions();
        final int partitionIndex = defineLastProcessedPartitionIndex();
        setPartitionsForJobIfNotExists(partitions);

        int percentage_10 = partitions.length / 10;
        int numberOfProcessedPartitions = 0;
        int totalPercentageDoneWork = 0;

        for (int partitionId = partitionIndex; partitionId < partitions.length; partitionId++) {
            final int partition = partitions[partitionId];

            // BUILD PHASE
            final HashMap<Integer, BinaryObject> hashTable = buildHashTable(partition);
            if (hashTable.isEmpty()) {
                continue;
            }

            List<BinaryObject> rightCacheEntries = scanRightCache(rightCache, hashTable.keySet());
            if (rightCacheEntries.isEmpty()) {
                continue;
            }
            List<PartPartSuppProj> batchResult = new ArrayList<>(rightCacheEntries.size());

            // PROBE PHASE
            final String surrogateKey = FaultTolerantUtils.generateSurrogateKey(taskId, jobId, partitionId);
            for (BinaryObject rightCacheEntry : rightCacheEntries) {
                BinaryObject foundPartBinaryObject = hashTable.get((Integer) rightCacheEntry.field("ps_partkey"));
                if (foundPartBinaryObject != null) {
                    // found match
                    batchResult.add(new PartPartSuppProj(
                        surrogateKey,
                        foundPartBinaryObject,
                        rightCacheEntry.field("ps_dummy")));
                }
            }
            jobCtx.setAttribute(JobUtils.getLastProcessedPartitionIndex(taskId, jobId), partitionId);
            failoverCache.putAsync(surrogateKey, batchResult);
            result.addAll(batchResult);
            numberOfProcessedPartitions += 1;
            if (numberOfProcessedPartitions % percentage_10 == 0) {
                totalPercentageDoneWork += 10;
                log.debug(String.format(PGW_FTJ + "Node [%s] processed [%d] percentage of partitions", nodeId, totalPercentageDoneWork));
            }
        }

        log.debug(PGW_FTJ + "Job [" + jobId + "] ended processing");
        return result;
    }

    private void setPartitionsForJobIfNotExists(int[] partitions) {
        final String jobProcessingPartitionsAttributeName = (String) FaultTolerantUtils
            .getJobProcessingPartitionsAttributeName(taskId, jobId);

        if (jobProcessingPartitionsAttributeName != null || jobProcessingPartitionsAttributeName == "") {
            jobCtx.setAttribute(FaultTolerantUtils.getJobProcessingPartitionsAttributeName(taskId, jobId), partitions);
        }
    }

    private int[] getPartitions() {
        int[] partitions = jobCtx.getAttribute(
            FaultTolerantUtils.getJobProcessingPartitionsAttributeName(taskId, jobId));
        if (partitions != null) {
            isRecoveryJob = true;
            log.debug(PGW_FTJ + "Loaded indexes of partitions from job context for job [" + jobId + "]. Recovery"
                + "job will start soon.");
            return partitions;
        }

        return ignite.affinity(this.cacheNameA).primaryPartitions(ignite.cluster().localNode());
    }

    private int defineLastProcessedPartitionIndex() {
        Integer lastProcessedPartitionIndex = jobCtx.getAttribute(
            JobUtils.getLastProcessedPartitionIndex(taskId, jobId));
        int index = lastProcessedPartitionIndex == null ? 0 : lastProcessedPartitionIndex + 1;
        log.debug(PGW_FTJ + "Previously, there have been processed " + index + " partitions");
        return index;
    }

    private List<BinaryObject> scanRightCache(
        IgniteCache<Integer, PartSupp> rightCache,
        final Set<Integer> keys) {

        final long start = System.currentTimeMillis();
        IgniteBiPredicate<Integer, BinaryObject> filter = (integer, binaryObject) ->
            keys.contains((Integer) binaryObject.field("ps_partkey"));

        List<BinaryObject> binaryObjects = rightCache.query(new ScanQuery<>(filter),
            (IgniteClosure<Entry<Integer, BinaryObject>, BinaryObject>) Entry::getValue).getAll();
        log.debug(PGW_FTJ + "Fetching right cache entries took " + (System.currentTimeMillis() - start) + " ms");
        log.debug(
            PGW_FTJ + "Job [" + jobId + "] loads " + binaryObjects.size() + " entries of " + cacheNameB + " cache");
        return binaryObjects;
    }

    private HashMap<Integer, BinaryObject> buildHashTable(int partition) {
        long start = System.currentTimeMillis();
        final HashMap<Integer, BinaryObject> hashTable = new HashMap<>(localHashTableCapacity);
        try (QueryCursor<Entry<Integer, BinaryObject>> cursor = leftCache
            .query(new ScanQuery<Integer, BinaryObject>(partition).setLocal(!isRecoveryJob))) {
            for (Entry<Integer, BinaryObject> entry : cursor) {
                BinaryObject entryValue = entry.getValue();
                hashTable.put((Integer) entryValue.field("p_partkey"), entryValue);
            }
        }
        log.debug(PGW_FTJ + "Building hash table took " + (System.currentTimeMillis() - start) + " ms");
        return hashTable;
    }

    @Override
    public void onMasterNodeLeft(ComputeTaskSession ses) throws IgniteException {

    }

}
