package ru.pgw.ftj.queries_seim.q14.hj;

import static ru.pgw.ftj.constants.PgwConstants.PGW_HJ;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import ru.pgw.ftj.MutablePair;
import ru.pgw.ftj.queries_seim.AbstractJoinComputeJob;

public class JoinComputeJob14 extends AbstractJoinComputeJob {

    private final String PROMO_PREFIX = "PROMO";

    private final LocalDate initShipDate;

    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public JoinComputeJob14(String cacheNameA, String cacheNameB,
        int localHashTableCapacity, String shipDate) {
        super.logPrefix = PGW_HJ;
        this.cacheNameA = cacheNameA;
        this.cacheNameB = cacheNameB;
        this.localHashTableCapacity = localHashTableCapacity;
        this.initShipDate = LocalDate.parse(shipDate, dateTimeFormatter);
    }

    private void initJobResources() {
        this.nodeId = ignite.cluster().localNode().id().toString();
        this.jobId = jobCtx.getJobId().toString();
        this.taskId = taskSession.getId().toString();
        this.leftCache = ignite.cache(this.cacheNameA).withKeepBinary();
        this.rightCache = ignite.cache(this.cacheNameB).withKeepBinary();
        jobCtx.setAttribute("alreadyFailed", false);
        log.debug(logPrefix + "Starting job " + jobId + " on node " + nodeId);
    }

    @Override
    public Object execute() throws IgniteException {
        initJobResources();

        int[] partitions = getPartitions();
        final int partitionIndex = defineLastProcessedPartitionIndex();
        setPartitionsForJobIfNotExists(partitions);

        int percentage_10 = partitions.length / 10;
        int numberOfProcessedPartitions = 0;
        int totalPercentageDoneWork = 0;

        final MutablePair<Float, Float> accumulator = new MutablePair<>(0F, 0F);
        for (int partitionId = partitionIndex; partitionId < partitions.length; partitionId++) {
            final int partition = partitions[partitionId];

            // BUILD PHASE
            final HashMap<Integer, BinaryObject> hashTable = buildHashTable(partition, "p_partkey");
            if (hashTable.isEmpty()) {
                continue;
            }

            List<BinaryObject> rightCacheEntries = scanRightCache(rightCache, "l_partkey", hashTable.keySet());
            if (rightCacheEntries.isEmpty()) {
                continue;
            }

            // PROBE PHASE
            for (BinaryObject rightCacheEntry : rightCacheEntries) {
                BinaryObject foundBinaryObject = hashTable.get((Integer) rightCacheEntry.field("l_partkey"));
                if (foundBinaryObject == null) {
                    continue;
                }

                // found match
                LocalDate shipDate = ((Date) rightCacheEntry.field("l_shipdate"))
                    .toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();

                if (!((shipDate.isAfter(initShipDate) || initShipDate.compareTo(shipDate) == 0)
                    && shipDate.isBefore(initShipDate.plusMonths(1)))) {
                    continue;
                }

                float extendedPrice = rightCacheEntry.field("l_extendedprice");
                float discount = rightCacheEntry.field("l_discount");
                final String partType = foundBinaryObject.field("p_type");
                float factor = extendedPrice * (1 - discount);
                int orderKey = rightCacheEntry.field("l_orderkey");
                log.debug(String.format(logPrefix + "orderKey[%d]", orderKey));
                accumulator.setLeft(accumulator.getLeft() + (partType.startsWith(PROMO_PREFIX) ? factor : 0));
                accumulator.setRight(accumulator.getRight() + factor);

                numberOfProcessedPartitions += 1;
                if (numberOfProcessedPartitions % percentage_10 == 0) {
                    totalPercentageDoneWork += 10;
                    log.debug(String.format(PGW_HJ + "Node [%s] processed [%d] percentage of partitions", nodeId, totalPercentageDoneWork));
                }
            }
        }

        log.debug(logPrefix + "Job [" + jobId + "] ended processing");
        return accumulator;
    }

}
