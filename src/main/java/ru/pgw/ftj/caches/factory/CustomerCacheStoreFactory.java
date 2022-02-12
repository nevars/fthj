package ru.pgw.ftj.caches.factory;

import javax.cache.configuration.Factory;
import ru.pgw.ftj.caches.cachestore.CustomerCacheStore;

public class CustomerCacheStoreFactory implements Factory<CustomerCacheStore> {

    @Override
    public CustomerCacheStore create() {
        return new CustomerCacheStore();
    }

}
