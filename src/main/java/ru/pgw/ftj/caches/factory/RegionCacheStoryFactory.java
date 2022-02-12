package ru.pgw.ftj.caches.factory;

import javax.cache.configuration.Factory;
import ru.pgw.ftj.caches.cachestore.RegionCacheStore;

public class RegionCacheStoryFactory implements Factory<RegionCacheStore> {

    @Override
    public RegionCacheStore create() {
        return new RegionCacheStore();
    }

}
