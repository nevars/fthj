package ru.pgw.ftj.join.join;

import static ru.pgw.ftj.constants.PgwConstants.CLIENT_CONFIG;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.jupiter.api.Test;
import ru.pgw.ftj.caches.filter.WorkerNodeFilter;
import ru.pgw.ftj.constants.PgwConstants;

public class BinaryObjectTest {

    CacheConfiguration createNewCacheConfiguration(CacheConfiguration copyFromConfiguration,
        String cacheName,
        int backupsNumber,
        String keyFieldName,
        LinkedHashMap<String, String> entityFields) {

        return new CacheConfiguration(copyFromConfiguration)
            .setName(cacheName)
            .setBackups(backupsNumber)
            .setWriteThrough(false)
            .setReadThrough(false)
            .setQueryEntities(Collections.singletonList(new QueryEntity()
                .setFields(entityFields)
                .setKeyFieldName(keyFieldName).setValueType(BinaryObject.class.getName())))
            .setNodeFilter(new WorkerNodeFilter());
    }

    @Test
    void checkBinaryObjectFieldNames() {
        try (Ignite ignite = Ignition.start(CLIENT_CONFIG)) {
            IgniteCache<Integer, BinaryObject> cacheA = ignite.getOrCreateCache(
                PgwConstants.REGION_CACHE_NAME).withKeepBinary();
            IgniteCache<Integer, BinaryObject> cacheB = ignite.getOrCreateCache(
                PgwConstants.NATION_CACHE_NAME).withKeepBinary();
            CacheConfiguration cacheAConfiguration = cacheA.getConfiguration(CacheConfiguration.class);
            CacheConfiguration cacheBConfiguration = cacheB.getConfiguration(CacheConfiguration.class);
            LinkedHashMap<String, String> fieldsCacheA = ((List<QueryEntity>) cacheAConfiguration.getQueryEntities()).get(0)
                .getFields();
            LinkedHashMap<String, String> fieldsCacheB = ((List<QueryEntity>) cacheBConfiguration.getQueryEntities()).get(0)
                .getFields();
            System.out.println();
        }
    }

}

