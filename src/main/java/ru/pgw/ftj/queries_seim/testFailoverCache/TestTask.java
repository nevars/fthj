package ru.pgw.ftj.queries_seim.testFailoverCache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.enums.PgwClusterRole;
import ru.pgw.ftj.projections.q0.PartPartSuppProj;

@ComputeTaskSessionFullSupport
public class TestTask extends ComputeTaskAdapter<Object, Object> {

    @IgniteInstanceResource
    private Ignite ignite;

    @Override
    public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg)
        throws IgniteException {

        final Map<TestJob, ClusterNode> jobToNode = new HashMap<>(subgrid.size());
        subgrid.stream().filter(clusterNode -> clusterNode
                .attributes()
                .containsKey(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME) &&
                PgwClusterRole.WORKER.toString()
                    .equals(clusterNode.attributes().get(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME)))
            .forEach(clusterNode -> jobToNode.put(new TestJob(), clusterNode));
        return jobToNode;
    }

    @Override
    public @Nullable Object reduce(List<ComputeJobResult> results) throws IgniteException {
        IgniteCache<Object, List<? extends Serializable>> failoverCache = ignite.getOrCreateCache(
            FaultTolerantConstants.FAILOVER_CACHE);
        failoverCache.put("finalReduceResult", new ArrayList<>());
        return results.stream()
            .map(ComputeJobResult::getData)
            .collect(Collectors.toList());
    }

    private static class TestJob extends ComputeJobAdapter {

        @JobContextResource
        private ComputeJobContext jobCtx;

        @IgniteInstanceResource
        private Ignite ignite;

        @Override
        public Object execute() throws IgniteException {
            IgniteCache<Object, List<? extends Serializable>> failoverCache = ignite.getOrCreateCache(
                FaultTolerantConstants.FAILOVER_CACHE);

            List<PartPartSuppProj> result = new ArrayList<>();
            result.add(new PartPartSuppProj("dummy1"));
            List<PartPartSuppProj> result2 = Arrays.asList(new PartPartSuppProj("dummy2"));
            failoverCache.put("testKey-" + jobCtx.getJobId().toString(), result);
            failoverCache.put("testKey-" + jobCtx.getJobId().toString(), result2);
            result.addAll(result2);

            return result;
        }

    }

}
