package ru.pgw.ftj.caches.cachestore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import ru.pgw.ftj.caches.PartSupp;

public class PartSuppCacheStore implements CacheStore<Integer, PartSupp> {

    @SpringResource(resourceName = "dataSource")
    private DataSource dataSource;

    @Override
    public void loadCache(IgniteBiInClosure<Integer, PartSupp> clo, @Nullable Object... args)
        throws CacheLoaderException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from partsupp")) {
                try (ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        PartSupp partsupp = new PartSupp(rs.getInt(1), rs.getString(6));
                        clo.apply(partsupp.getPs_partkey(), partsupp);
                    }
                }
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from partsupp cache store.", e);
        }
    }

    @Override
    public void sessionEnd(boolean commit) throws CacheWriterException {

    }

    @Override
    public PartSupp load(Integer key) throws CacheLoaderException {
        return null;
    }

    @Override
    public Map<Integer, PartSupp> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
        return null;
    }

    @Override
    public void write(Entry<? extends Integer, ? extends PartSupp> entry) throws CacheWriterException {

    }

    @Override
    public void writeAll(Collection<Entry<? extends Integer, ? extends PartSupp>> entries) throws CacheWriterException {

    }

    @Override
    public void delete(Object key) throws CacheWriterException {

    }

    @Override
    public void deleteAll(Collection<?> keys) throws CacheWriterException {

    }

}
