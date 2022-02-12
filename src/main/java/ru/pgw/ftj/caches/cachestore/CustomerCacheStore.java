package ru.pgw.ftj.caches.cachestore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Map;
import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.SpringResource;
import org.jetbrains.annotations.Nullable;
import ru.pgw.ftj.caches.Customer;

public class CustomerCacheStore implements CacheStore<Integer, Customer> {

    @SpringResource(resourceName = "dataSource")
    private DataSource dataSource;

    @Override
    public void loadCache(IgniteBiInClosure<Integer, Customer> clo, @Nullable Object... args)
        throws CacheLoaderException {

        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from customer")) {
                try (ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        Customer customer = new Customer(rs.getInt(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getInt(4),
                            rs.getString(5),
                            rs.getString(7),
                            rs.getString(8),
                            rs.getString(9));
                        clo.apply(customer.getC_custkey(), customer);
                    }
                }
            }
        } catch (Exception e) {
            throw new CacheLoaderException("Failed to load values from CUSTOMER cache store.", e);
        }
    }

    @Override
    public void sessionEnd(boolean commit) throws CacheWriterException {

    }

    @Override
    public Customer load(Integer key) throws CacheLoaderException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from customer where c_custkey=" + key)) {
                try (ResultSet rs = st.executeQuery()) {
                    return new Customer(rs.getInt(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getInt(4),
                        rs.getString(5),
                        rs.getString(7),
                        rs.getString(8),
                        rs.getString(9));
                }
            }
        } catch (Exception e) {
            throw new CacheLoaderException("Failed to load values from CUSTOMER cache store.", e);
        }
    }

    @Override
    public Map<Integer, Customer> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
        return null;
    }

    @Override
    public void write(Entry<? extends Integer, ? extends Customer> entry) throws CacheWriterException {

    }

    @Override
    public void writeAll(Collection<Entry<? extends Integer, ? extends Customer>> entries) throws CacheWriterException {

    }

    @Override
    public void delete(Object key) throws CacheWriterException {

    }

    @Override
    public void deleteAll(Collection<?> keys) throws CacheWriterException {

    }

}
