package ru.pgw.ftj.caches.factory;

import javax.cache.configuration.Factory;
import ru.pgw.ftj.caches.cachestore.PartCacheStore;

public class PartCacheStoreFactory implements Factory<PartCacheStore> {

    @Override
    public PartCacheStore create() {
        return new PartCacheStore();
    }

}
