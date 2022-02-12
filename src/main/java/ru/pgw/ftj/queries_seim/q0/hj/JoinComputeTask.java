package ru.pgw.ftj.queries_seim.q0.hj;

import static ru.pgw.ftj.constants.PgwConstants.PGW_HJ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.pgw.ftj.constants.PgwConstants;
import ru.pgw.ftj.projections.q0.PartPartSuppProj;

@ComputeTaskSessionFullSupport
public class JoinComputeTask extends ComputeTaskAdapter<Object, Object> {

    @IgniteInstanceResource
    private Ignite ignite;

    @LoggerResource
    private IgniteLogger log;

    // expected number of entries in the result list
    private final int resultListCapacity;

    public JoinComputeTask(int resultListCapacity) {
        this.resultListCapacity = resultListCapacity;
    }

    @Override
    public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> clusterNodes, @Nullable Object arg)
        throws IgniteException {

        final Map<JoinComputeJob, ClusterNode> jobToNode = new HashMap<>(clusterNodes.size());
        final long start = System.currentTimeMillis();
        clusterNodes.forEach(clusterNode ->
            jobToNode.put(new JoinComputeJob(PgwConstants.PART_CACHE_NAME,
                    PgwConstants.PART_SUPP_CACHE_NAME,
                    UUID.randomUUID().toString(), 1000),
                clusterNode)
        );

        log.debug(PGW_HJ + "MAP PHASE took " + (System.currentTimeMillis() - start) + " ms");
        return jobToNode;
    }

    @Override
    public @Nullable Object reduce(List<ComputeJobResult> results) throws IgniteException {
        List<PartPartSuppProj> result = new ArrayList<>(resultListCapacity);
        final long start = System.currentTimeMillis();
        for (ComputeJobResult jobResult : results) {
            List<PartPartSuppProj> jobResultData = jobResult.getData();
            log.debug(PGW_HJ + "Job completed on node " + jobResult.getNode().id() +
                " with number of entries " + jobResultData.size());
            result.addAll(jobResultData);

        }
        log.debug(PGW_HJ + "REDUCE PHASE took " + (System.currentTimeMillis() - start) + " ms");
        return result;
    }

}
