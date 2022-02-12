package ru.pgw.ftj.queries;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import javax.cache.Cache.Entry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
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
import ru.pgw.ftj.constants.PgwConstants;
import ru.pgw.ftj.util.FaultTolerantUtils;
import ru.pgw.ftj.util.JobUtils;

public abstract class AbstractComputeJoinJob extends ComputeJobAdapter {

    @TaskSessionResource
    protected ComputeTaskSession taskSession;

    @JobContextResource
    protected ComputeJobContext jobCtx;

    @IgniteInstanceResource
    protected Ignite ignite;

    @LoggerResource
    protected IgniteLogger log;

    protected String nodeId;

    protected String jobId;

    protected String taskId;

    protected boolean isRecoveryJob = false;

    protected String leftCacheName;

    protected String rightCacheName;

    protected IgniteCache<Object, BinaryObject> leftCache;

    protected IgniteCache<Object, BinaryObject> rightCache;

    protected int localHashTableCapacity;

    protected String logPrefix = PgwConstants.PGW_DEFAULT_LOG_PREFIX;

    public AbstractComputeJoinJob(String leftCacheName, String rightCacheName) {
        this.leftCacheName = leftCacheName;
    }

    protected void setPartitionsForJobIfNotExists(int[] partitions) {
        final String jobProcessingPartitionsAttributeName = (String) FaultTolerantUtils
            .getJobProcessingPartitionsAttributeName(taskId, jobId);

        if (jobProcessingPartitionsAttributeName != null || jobProcessingPartitionsAttributeName == "") {
            jobCtx.setAttribute(FaultTolerantUtils.getJobProcessingPartitionsAttributeName(taskId, jobId), partitions);
        }
    }

    protected int[] getPartitions() {
        int[] partitions = jobCtx.getAttribute(
            FaultTolerantUtils.getJobProcessingPartitionsAttributeName(taskId, jobId));
        if (partitions != null) {
            isRecoveryJob = true;
            log.debug(logPrefix + "Loaded indexes of partitions from job context for job [" + jobId + "]. Recovery"
                + "job will start soon.");
            return partitions;
        }

        return ignite.affinity(this.leftCacheName).primaryPartitions(ignite.cluster().localNode());
    }

    protected int defineLastProcessedPartitionIndex() {
        Integer lastProcessedPartitionIndex = jobCtx.getAttribute(
            JobUtils.getLastProcessedPartitionIndex(taskId, jobId));
        int index = lastProcessedPartitionIndex == null ? 0 : lastProcessedPartitionIndex + 1;
        log.debug(logPrefix + "Previously, there have been processed " + index + " partitions");
        return index;
    }

    protected HashMap<Integer, BinaryObject> buildHashTable(int partition, String keyFieldName) {
        long start = System.currentTimeMillis();
        final HashMap<Integer, BinaryObject> hashTable = new HashMap<>(localHashTableCapacity);
        try (QueryCursor<Entry<Integer, BinaryObject>> cursor = leftCache
            .query(new ScanQuery<Integer, BinaryObject>(partition).setLocal(!isRecoveryJob))) {
            for (Entry<Integer, BinaryObject> entry : cursor) {
                BinaryObject entryValue = entry.getValue();
                hashTable.put((Integer) entryValue.field(keyFieldName), entryValue);
            }
        }
        log.debug(logPrefix + "Building hash table took " + (System.currentTimeMillis() - start) + " ms");
        return hashTable;
    }

    protected List<BinaryObject> scanRightCache(
        IgniteCache<Object, ? extends Serializable> rightCache, String field, final Set<Integer> keys) {

        final long start = System.currentTimeMillis();
        IgniteBiPredicate<Integer, BinaryObject> filter = (integer, binaryObject) ->
            keys.contains((Integer) binaryObject.field(field));

        List<BinaryObject> binaryObjects = rightCache.query(new ScanQuery<>(filter),
            (IgniteClosure<Entry<Integer, BinaryObject>, BinaryObject>) Entry::getValue).getAll();
        log.debug(logPrefix + "Fetching right cache entries took " + (System.currentTimeMillis() - start) + " ms");
        log.debug(
            logPrefix + "Job [" + jobId + "] loads " + binaryObjects.size() + " entries of " + rightCacheName + " cache");
        return binaryObjects;
    }

}
