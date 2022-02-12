package ru.pgw.ftj.queries_seim.q14.hj;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.pgw.ftj.MutablePair;
import ru.pgw.ftj.constants.PgwConstants;
import ru.pgw.ftj.queries_seim.AbstractJoinComputeTask;

public class JoinComputeTask14 extends AbstractJoinComputeTask {

    private final String shipDate;

    public JoinComputeTask14(String shipDate) {
        this.shipDate = shipDate;
        super.logPrefix = PgwConstants.PGW_DEFAULT_LOG_PREFIX;
    }

    @Override
    public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> workers, @Nullable Object arg)
        throws IgniteException {

        final Map<JoinComputeJob14, ClusterNode> jobToNode = new HashMap<>(workers.size());

        final long start = System.currentTimeMillis();
        workers.forEach(clusterNode ->
            jobToNode.put(new JoinComputeJob14(PgwConstants.PART_CACHE_NAME,
                    PgwConstants.LINE_ITEM_CACHE_NAME, 1000, shipDate),
                clusterNode)
        );

        log.debug(logPrefix + "MAP PHASE took " + (System.currentTimeMillis() - start) + " ms");
        return jobToNode;
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

        log.debug(logPrefix + "REDUCE PHASE took " + (System.currentTimeMillis() - start) + " ms");
        return 100 * (numerator / denominator);
    }

}
