package ru.pgw.ftj.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;

public final class StatisticUtils {

    public static void main(String[] args) {
        Map<String, Object> joinStatistic = getJoinStatisticFromFile(args[0], args[1]);
        System.out.println("----------- JOIN STATISTIC LOOKS LIKE THAT -----------");
        joinStatistic.forEach((key, value) -> {
            System.out.println(key + ": " + value);
        });
        System.out.println("----------------- END JOIN STATISTIC -----------------");
    }

    private static Map<String, Object> getJoinStatisticFromFile(
        @NonNull String igniteLogFile,
        @NonNull String particularIgniteLogFile) {

        final Map<String, Object> statistic = new HashMap<>();

        final Path igniteLogFilePath = Paths.get(igniteLogFile);
        statistic.put("Time to build hash table (msc)",
            getSumOfMillisecondsAfterPhrase(igniteLogFilePath, "Building hash table took"));
        statistic.put("Fetching right cache entries took (msc)",
            getSumOfMillisecondsAfterPhrase(igniteLogFilePath, "Fetching right cache entries took"));

        final Path particularIgniteLogFilePath = Paths.get(particularIgniteLogFile);
        statistic.put("Map phase took (msc)", getTimeToExecutePhase(particularIgniteLogFilePath, "MAP PHASE took"));
        statistic.put("Reduce phase took (msc)",
            getTimeToExecutePhase(particularIgniteLogFilePath, "REDUCE PHASE took"));
        return statistic;
    }

    private static int getSumOfMillisecondsAfterPhrase(@NonNull Path filePath, @NonNull String phrase) {
        int milliseconds = -1;
        try {
            milliseconds = Files.lines(filePath)
                .filter(line -> line.contains(phrase) && line.contains("pub-#106%pgw-grid-server0%"))
                .map(line -> line.substring(line.lastIndexOf(phrase) + phrase.length()).split(" ")[1])
                .mapToInt(Integer::valueOf)
                .sum();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return milliseconds;
    }

    private static int getTimeToExecutePhase(@NonNull Path filePath, @NonNull String phase) {
        int milliseconds = -1;
        try {
            milliseconds = Files.lines(filePath)
                .filter(line -> line.contains(phase))
                .map(line -> line.substring(line.lastIndexOf(phase)).split(" ")[3])
                .mapToInt(Integer::valueOf)
                .findFirst()
                .getAsInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return milliseconds;
    }

}
