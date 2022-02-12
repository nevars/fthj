package ru.pgw.ftj.queries_seim.q0.ftj;

import static ru.pgw.ftj.constants.PgwConstants.PGW_FTJ;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import javax.cache.Cache.Entry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.constants.PgwConstants;
import ru.pgw.ftj.enums.PgwClusterRole;
import ru.pgw.ftj.projections.q0.PartPartSuppProj;

@ComputeTaskSessionFullSupport
public class FtjJoinComputeTask extends ComputeTaskAdapter<Object, Object> {

    @IgniteInstanceResource
    private Ignite ignite;

    @LoggerResource
    private IgniteLogger log;

    // expected number of entries in the result list
    private final int resultListCapacity;

    private final CopyOnWriteArrayList<PartPartSuppProj> recoveredJoinData;

    private IgniteCache<String, List<? extends Serializable>> failoverCache;

    public FtjJoinComputeTask(int resultListCapacity) {
        this.resultListCapacity = resultListCapacity;
        this.recoveredJoinData = new CopyOnWriteArrayList();
    }

    @Override
    public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> workers, @Nullable Object arg)
        throws IgniteException {
        this.failoverCache = ignite.cache(FaultTolerantConstants.FAILOVER_CACHE);
        final Map<FtjJoinComputeJob, ClusterNode> jobToNode = new HashMap<>(workers.size());

        final long start = System.currentTimeMillis();
        workers.forEach(clusterNode ->
            jobToNode.put(new FtjJoinComputeJob(PgwConstants.PART_CACHE_NAME,
                    PgwConstants.PART_SUPP_CACHE_NAME, 1000),
                clusterNode)
        );

        log.debug(PGW_FTJ + "MAP PHASE took " + (System.currentTimeMillis() - start) + " ms");
        return jobToNode;
    }

    @Override
    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        IgniteException jobException = res.getException();

        if (jobException == null) {
            // all is good, not any error occurred.
            return ComputeJobResultPolicy.WAIT;
        }

        final String taskJobIdSuffix = res.getJobContext()
            .getAttribute(FaultTolerantConstants.TASK_JOB_ID_ATTRIBUTE_NAME);
        IgniteFuture<Object> objectIgniteFuture = ignite.compute(ignite
                .cluster()
                .forAttribute(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME,
                    PgwClusterRole.DATA_KEEPER.toString()))
            .applyAsync((IgniteClosure<Ignite, Object>) ignite -> {
                IgniteBiPredicate<String, Object> filter = (s, partPartSuppProj) ->
                    s.startsWith(taskJobIdSuffix);
                return ignite.cache(FaultTolerantConstants.FAILOVER_CACHE)
                    .query(new ScanQuery<>(filter))
                    .getAll();
            }, ignite);

        objectIgniteFuture.listen(future -> {
            List<Entry<Object, Object>> recoveredData = (List<Entry<Object, Object>>) future.get();
            List<PartPartSuppProj> rowData = recoveredData
                .stream()
                .map(entry -> (List<PartPartSuppProj>) entry.getValue())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
            log.debug(PGW_FTJ + "Loaded from the recovery cache " + rowData.size() + " entries");
            recoveredJoinData.addAll(rowData);
        });

        log.debug(
            PGW_FTJ + "REACHED RESULT METHOD TO RE-RUN A FAILED JOB " + res.getJobContext().getJobId().toString());
        res.getJobContext()
            .setAttribute(FaultTolerantConstants.TIME_TO_RECOVER_JOD_ATTRIBUTE_NAME, System.currentTimeMillis());
        return ComputeJobResultPolicy.FAILOVER;
    }

    @Override
    public @Nullable Object reduce(List<ComputeJobResult> results) throws IgniteException {
        List<PartPartSuppProj> result = new ArrayList<>(resultListCapacity);
        final long start = System.currentTimeMillis();
        for (ComputeJobResult jobResult : results) {

            List<PartPartSuppProj> jobResultData = jobResult.getData();
            log.debug(PGW_FTJ + "Job completed on node " + jobResult.getNode().id() +
                " with number of entries " + jobResultData.size());
            result.addAll(jobResultData);

        }
        log.debug(PGW_FTJ + "REDUCE PHASE took " + (System.currentTimeMillis() - start) + " ms");
        log.debug(PGW_FTJ + "The number of recovered data: " + recoveredJoinData.size() + " entries.");
        result.addAll(recoveredJoinData);
        return result;
    }

}
