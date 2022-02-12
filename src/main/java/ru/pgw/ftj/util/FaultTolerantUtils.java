package ru.pgw.ftj.util;

import ru.pgw.ftj.constants.FaultTolerantConstants;
import ru.pgw.ftj.constants.PgwConstants;

public final class FaultTolerantUtils {

    public static String generateSurrogateKey(String taskId, String jobId) {
        return taskId + PgwConstants.DELIMETER + jobId;
    }

    public static String generateSurrogateKey(String taskId, String jobId, int partitionId) {
        return generateSurrogateKey(taskId, jobId) + PgwConstants.DELIMETER + partitionId;
    }

    public static Object getJobProcessingPartitionsAttributeName(String taskId, String jobId) {
        return taskId + PgwConstants.DELIMETER + jobId + PgwConstants.DELIMETER + FaultTolerantConstants.JOB_PARTITIONS;
    }

}
