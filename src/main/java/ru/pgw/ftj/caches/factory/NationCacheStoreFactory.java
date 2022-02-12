package ru.pgw.ftj.caches.factory;

import javax.cache.configuration.Factory;
import ru.pgw.ftj.caches.cachestore.NationCacheStore;

public class NationCacheStoreFactory implements Factory<NationCacheStore> {

    @Override
    public NationCacheStore create() {
        return new NationCacheStore();
    }

}
