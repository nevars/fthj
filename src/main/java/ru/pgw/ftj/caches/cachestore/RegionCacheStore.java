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
import ru.pgw.ftj.caches.Region;

public class RegionCacheStore implements CacheStore<Integer, Region> {

    @SpringResource(resourceName = "dataSource")
    private DataSource dataSource;

    @Override
    public void loadCache(IgniteBiInClosure<Integer, Region> clo, @Nullable Object... args)
        throws CacheLoaderException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from region")) {
                try (ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        Region region = new Region(
                            rs.getInt(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getString(4));
                        clo.apply(region.getR_regionkey(), region);
                    }
                }
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from REGION cache store.", e);
        }
    }

    @Override
    public void sessionEnd(boolean commit) throws CacheWriterException {

    }

    @Override
    public Region load(Integer key) throws CacheLoaderException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from region where r_regionkey=" + key)) {
                try (ResultSet rs = st.executeQuery()) {
                    Region region = new Region(
                        rs.getInt(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4));
                    return region;
                }
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from REGION cache store.", e);
        }
    }

    @Override
    public Map<Integer, Region> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
        return null;
    }

    @Override
    public void write(Entry<? extends Integer, ? extends Region> entry) throws CacheWriterException {

    }

    @Override
    public void writeAll(Collection<Entry<? extends Integer, ? extends Region>> entries) throws CacheWriterException {

    }

    @Override
    public void delete(Object key) throws CacheWriterException {

    }

    @Override
    public void deleteAll(Collection<?> keys) throws CacheWriterException {

    }

}
