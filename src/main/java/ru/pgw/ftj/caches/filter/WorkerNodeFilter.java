package ru.pgw.ftj.caches.filter;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;
import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.enums.PgwClusterRole;

public class WorkerNodeFilter implements IgnitePredicate<ClusterNode> {

    @Override
    public boolean apply(ClusterNode clusterNode) {
        return clusterNode.attributes()
            .containsKey(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME) &&
            PgwClusterRole.WORKER.toString()
                .equals(clusterNode.attributes().get(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME));
    }

}
