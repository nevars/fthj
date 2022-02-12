package ru.pgw.ftj.queries_seim.q14.ftj;

import static ru.pgw.ftj.constants.PgwConstants.CLIENT_CONFIG;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.Cache.Entry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteClosure;
import ru.pgw.ftj.MutablePair;
import ru.pgw.ftj.constants.PgwConstants;

public class TestQ14 {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final LocalDate initShipDate = LocalDate.parse("1995-09-01", dateTimeFormatter);;

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start(CLIENT_CONFIG)) {
            IgniteCache<Integer, BinaryObject> cacheA = ignite.getOrCreateCache(PgwConstants.PART_CACHE_NAME).withKeepBinary();
            IgniteCache<Integer, BinaryObject> cacheB = ignite.getOrCreateCache(PgwConstants.LINE_ITEM_CACHE_NAME).withKeepBinary();

            cacheA.loadCache(null);
            System.out.println("CACHE PART IS FILLED");
            cacheB.loadCache(null);
            System.out.println("CACHE LINE ITEM IS FILLED");

            Map<Object, BinaryObject> hashTable = new HashMap<>(200_000);
            cacheA.query(new ScanQuery<>(),
                (IgniteClosure<Entry<Integer, BinaryObject>, BinaryObject>) Entry::getValue)
                .getAll()
                .forEach(entry -> {
                    hashTable.put(entry.field("p_partkey"), entry);
            });
            System.out.println("PART entries IS LOADED");

            List<BinaryObject> cacheB_entries = cacheB.query(new ScanQuery<>(),
                (IgniteClosure<Entry<Integer, BinaryObject>, BinaryObject>) Entry::getValue).getAll();
            System.out.println("LINE ITEM entries IS LOADED");

            final MutablePair<Float, Float> accumulator = new MutablePair<>(0F, 0F);
            for (BinaryObject bEntry : cacheB_entries) {

                BinaryObject p = hashTable.get(bEntry.field("l_partkey"));

                LocalDate shipDate = ((Date) bEntry.field("l_shipdate"))
                    .toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();

                if (!((shipDate.isAfter(initShipDate) || initShipDate.compareTo(shipDate) == 0)
                    && shipDate.isBefore(initShipDate.plusMonths(1)))) {
                    continue;
                }

                float extendedPrice = bEntry.field("l_extendedprice");
                float discount = bEntry.field("l_discount");
                final String partType = p.field("p_type");
                float factor = extendedPrice * (1 - discount);
                accumulator.setLeft(accumulator.getLeft() + (partType.startsWith("PROMO") ? factor : 0));
                accumulator.setRight(accumulator.getRight() + factor);
            }
            System.out.println("RESULT = " + 100 * (accumulator.getLeft() / accumulator.getRight()));
        }
    }
}
