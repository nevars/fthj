package ru.pgw.ftj.helpers;

import java.util.Collections;
import java.util.LinkedHashMap;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import ru.pgw.ftj.caches.Part;
import ru.pgw.ftj.caches.PartSupp;
import ru.pgw.ftj.caches.factory.PartCacheStoreFactory;
import ru.pgw.ftj.caches.factory.PartSuppCacheStoreFactory;
import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.constants.PgwConstants;
import ru.pgw.ftj.enums.PgwClusterRole;

public class CacheHelper {

    private static QueryEntity createPartSuppEntity() {
        QueryEntity part = new QueryEntity(Integer.class, PartSupp.class);
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("ps_partkey", Integer.class.getName());
        fields.put("ps_dummy", String.class.getName());
        part.setFields(fields);
        part.setIndexes(Collections.singletonList(new QueryIndex("ps_partkey")));
        return part;
    }

    private static CacheConfiguration createCacheConfig_PartSuppCache() {
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName(PgwConstants.PART_SUPP_CACHE_NAME);
        cacheConfiguration.setBackups(1);
        //cacheConfiguration.setSqlSchema("postgres");
        cacheConfiguration.setReadThrough(false);
        cacheConfiguration.setWriteThrough(false);
        cacheConfiguration.setCacheLoaderFactory(new PartSuppCacheStoreFactory());
        cacheConfiguration.setNodeFilter((IgnitePredicate<ClusterNode>) clusterNode ->
            clusterNode.attributes()
                .containsKey(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME) &&
                PgwClusterRole.WORKER.toString()
                    .equals(clusterNode.attributes().get(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME)));

        cacheConfiguration.setQueryEntities(Collections.singletonList(createPartSuppEntity()));

        return cacheConfiguration;
    }

    private static QueryEntity createPartEntity() {
        QueryEntity part = new QueryEntity(Integer.class, Part.class);
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("p_partkey", Integer.class.getName());
        fields.put("p_name", String.class.getName());
        part.setFields(fields);
        part.setIndexes(Collections.singletonList(new QueryIndex("p_partkey")));
        return part;
    }

    private static CacheConfiguration createCacheConfig_PartCache() {
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName(PgwConstants.PART_CACHE_NAME);
        cacheConfiguration.setBackups(1);
        //cacheConfiguration.setSqlSchema("postgres");
        cacheConfiguration.setReadThrough(false);
        cacheConfiguration.setWriteThrough(false);
        cacheConfiguration.setCacheLoaderFactory(new PartCacheStoreFactory());
        cacheConfiguration.setNodeFilter((IgnitePredicate<ClusterNode>) clusterNode ->
            clusterNode.attributes()
                .containsKey(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME) &&
                PgwClusterRole.WORKER.toString()
                    .equals(clusterNode.attributes().get(FaultTolerantConstants.CLUSTER_NODE_ROLE_ATTRIBUTE_NAME)));

        cacheConfiguration.setQueryEntities(Collections.singletonList(createPartEntity()));

        return cacheConfiguration;
    }

}
