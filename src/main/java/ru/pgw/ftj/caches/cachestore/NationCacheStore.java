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
import ru.pgw.ftj.caches.Nation;

public class NationCacheStore implements CacheStore<Integer, Nation> {

    @SpringResource(resourceName = "dataSource")
    private DataSource dataSource;

    @Override
    public void loadCache(IgniteBiInClosure<Integer, Nation> clo, @Nullable Object... args)
        throws CacheLoaderException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from nation")) {
                try (ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        Nation nation = new Nation(rs.getInt(1),
                            rs.getString(2),
                            rs.getInt(3),
                            rs.getString(4),
                            rs.getString(5));
                        clo.apply(nation.getN_nationkey(), nation);
                    }
                }
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from NATION cache store.", e);
        }
    }

    @Override
    public void sessionEnd(boolean commit) throws CacheWriterException {

    }

    @Override
    public Nation load(Integer key) throws CacheLoaderException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from nation where n_nationkey=" + key)) {
                try (ResultSet rs = st.executeQuery()) {
                    return new Nation(rs.getInt(1),
                        rs.getString(2),
                        rs.getInt(3),
                        rs.getString(4),
                        rs.getString(5));
                }
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from NATION cache store.", e);
        }
    }

    @Override
    public Map<Integer, Nation> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
        return null;
    }

    @Override
    public void write(Entry<? extends Integer, ? extends Nation> entry) throws CacheWriterException {

    }

    @Override
    public void writeAll(Collection<Entry<? extends Integer, ? extends Nation>> entries) throws CacheWriterException {

    }

    @Override
    public void delete(Object key) throws CacheWriterException {

    }

    @Override
    public void deleteAll(Collection<?> keys) throws CacheWriterException {

    }

}
