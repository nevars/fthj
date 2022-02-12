package ru.pgw.ftj.caches.factory;

import javax.cache.configuration.Factory;
import ru.pgw.ftj.caches.cachestore.PartSuppCacheStore;

public class PartSuppCacheStoreFactory implements Factory<PartSuppCacheStore> {

    @Override
    public PartSuppCacheStore create() {
        return new PartSuppCacheStore();
    }

}
