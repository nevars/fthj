package ru.pgw.ftj.queries_seim.q14.ftj;

import static ru.pgw.ftj.constants.PgwConstants.PGW_FTJ;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import javax.cache.Cache.Entry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.pgw.ftj.MutablePair;
import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.constants.PgwConstants;
import ru.pgw.ftj.queries_seim.AbstractJoinComputeTask;

public class FtjJoinComputeTask14 extends AbstractJoinComputeTask {

    private final String shipDate;

    private IgniteCache<String, MutablePair<Float, Float>> failoverCache;

    private CopyOnWriteArrayList<MutablePair<Float, Float>> recoveredJoinData = new CopyOnWriteArrayList<>();

    public FtjJoinComputeTask14(String shipDate) {
        this.shipDate = shipDate;
        super.logPrefix = PGW_FTJ;
    }

    @Override
    public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> workers, @Nullable Object arg)
        throws IgniteException {

        initFailoverCache();
        this.failoverCache = ignite.getOrCreateCache(FaultTolerantConstants.FAILOVER_CACHE);
        final Map<FtjJoinComputeJob14, ClusterNode> jobToNode = new HashMap<>(workers.size());

        final long start = System.currentTimeMillis();
        workers.forEach(clusterNode ->
            jobToNode.put(new FtjJoinComputeJob14(PgwConstants.PART_CACHE_NAME,
                    PgwConstants.LINE_ITEM_CACHE_NAME, 1000, shipDate),
                clusterNode)
        );

        log.debug(logPrefix + "MAP PHASE took " + (System.currentTimeMillis() - start) + " ms");
        return jobToNode;
    }

    @Override
    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        if (res.getException() == null) {
            // all is good, not any error occurred.
            return ComputeJobResultPolicy.WAIT;
        }

        final String taskJobIdSuffix = res.getJobContext()
            .getAttribute(FaultTolerantConstants.TASK_JOB_ID_ATTRIBUTE_NAME);
        IgniteFuture<Object> objectIgniteFuture = recoverJobData(taskJobIdSuffix);

        objectIgniteFuture.listen(future -> {
            List<Entry<Object, Object>> recoveredData = (List<Entry<Object, Object>>) future.get();
            List<MutablePair<Float, Float>> rowData = recoveredData
                .stream()
                .map(entry -> (List<MutablePair<Float, Float>>) entry.getValue())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
            log.debug(logPrefix + "Loaded from the recovery cache " + rowData.size() + " entries");
            recoveredJoinData.addAll(rowData);
        });

        log.debug(logPrefix + "REACHED RESULT METHOD TO RE-RUN A FAILED JOB " +
            res.getJobContext().getJobId().toString());
        res.getJobContext()
            .setAttribute(FaultTolerantConstants.TIME_TO_RECOVER_JOD_ATTRIBUTE_NAME, System.currentTimeMillis());
        return ComputeJobResultPolicy.FAILOVER;
    }

    @Override
    public @Nullable Object reduce(List<ComputeJobResult> results) throws IgniteException {
        float numerator = 0;
        float denominator = 0;
        final long start = System.currentTimeMillis();
        for (ComputeJobResult jobResult : results) {
            MutablePair<Float, Float> jobRes = jobResult.getData();
            numerator += jobRes.getLeft();
            denominator += jobRes.getRight();
        }
        if (!recoveredJoinData.isEmpty()) {
            for (MutablePair<Float, Float> pair : recoveredJoinData) {
                numerator += pair.getLeft();
                denominator += pair.getRight();
            }
        }
        log.debug(logPrefix + "REDUCE PHASE took " + (System.currentTimeMillis() - start) + " ms");
        log.debug(logPrefix + "The number of recovered data: " + recoveredJoinData.size() + " entries.");
        return 100 * (numerator / denominator);
    }

}
