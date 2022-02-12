package ru.pgw.ftj.util;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import ru.pgw.ftj.constants.FaultTolerantConstants;

public class ClusterUtils {

    public static void displayClusterNodePartitions(Ignite ignite, String... cacheNames) {
        Collection<ClusterNode> nodes = ignite.cluster().nodes();
        for (ClusterNode node : nodes) {
            String role = (String) node.attributes().get(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME);
            String nodeId = node.id().toString();
            System.out.println("Node[" + nodeId + "] with role[" + role + "] has partitions:");
            for (String cacheName : cacheNames) {
                int num = ignite.affinity(cacheName).primaryPartitions(node).length;
                System.out.println("Cache[" + cacheName + "] has " + num + " partitions");
            }

            System.out.println();
        }
    }

}
