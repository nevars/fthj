package ru.pgw.ftj.util;

import static ru.pgw.ftj.constants.FaultTolerantConstants.LAST_PROCESSED_PARTITION;
import static ru.pgw.ftj.constants.PgwConstants.DELIMETER;

public final class JobUtils {

    public static String getLastProcessedPartitionIndex(String taskId, String jobId) {
        return taskId + DELIMETER + jobId + LAST_PROCESSED_PARTITION;
    }

}
