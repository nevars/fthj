package ru.pgw.ftj.caches.cachestore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
import ru.pgw.ftj.caches.LineItem;

public class LineItemCacheStore implements CacheStore<Integer, LineItem> {

    @SpringResource(resourceName = "dataSource")
    private DataSource dataSource;

    @Override
    public void loadCache(IgniteBiInClosure<Integer, LineItem> clo, @Nullable Object... args)
        throws CacheLoaderException {

        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from lineitem")) {
                try (ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        final int id = rs.getInt(2);
                        LineItem lineItem = new LineItem(
                            id,
                            rs.getInt(1),
                            rs.getFloat(6),
                            rs.getFloat(7),
                            new SimpleDateFormat("yyyy-MM-dd").parse(rs.getString(11)));
                        clo.apply(id, lineItem);
                    }
                }
            }
        } catch (Exception e) {
            throw new CacheLoaderException("Failed to load values from part cache store.", e);
        }
    }

    @Override
    public void sessionEnd(boolean commit) throws CacheWriterException {

    }

    @Override
    public LineItem load(Integer key) throws CacheLoaderException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from lineitem where l_partkey=" + key)) {
                try (ResultSet rs = st.executeQuery()) {
                    LineItem lineItem = new LineItem(
                        rs.getInt(2),
                        rs.getInt(1),
                        rs.getFloat(6),
                        rs.getFloat(7),
                        new SimpleDateFormat("yyyy-MM-dd").parse(rs.getString(11)));
                    return lineItem;
                }
            }
        } catch (Exception e) {
            throw new CacheLoaderException("Failed to load values from part cache store.", e);
        }
    }

    @Override
    public Map<Integer, LineItem> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
        return null;
    }

    @Override
    public void write(Entry<? extends Integer, ? extends LineItem> entry) throws CacheWriterException {

    }

    @Override
    public void writeAll(Collection<Entry<? extends Integer, ? extends LineItem>> entries) throws CacheWriterException {

    }

    @Override
    public void delete(Object key) throws CacheWriterException {

    }

    @Override
    public void deleteAll(Collection<?> keys) throws CacheWriterException {

    }

}
