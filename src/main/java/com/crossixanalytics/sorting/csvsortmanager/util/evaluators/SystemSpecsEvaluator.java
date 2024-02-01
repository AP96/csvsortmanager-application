package com.crossixanalytics.sorting.csvsortmanager.util.evaluators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * A Utility class for evaluating file processing conditions according to the computer's system specifications.
 */
public class SystemSpecsEvaluator {
    private static final Logger logger = LoggerFactory.getLogger(SystemSpecsEvaluator.class);
    private static final int NUM_OF_BYTES_PER_INT_RECORD = 4;
    private static final double SAFETY_THRESHOLD = 0.7;
    private static final double PARTITION_PERCENTAGE = 0.05; // 5%

    /**
     * Calculates the maximum number of records that can be processed based on system memory and disk space.
     *
     * @param directoryPath The directory path to evaluate disk space.
     * @return The maximum number of records that can be handled.
     */
    public static long calculateMaxRecords(String directoryPath) {
        long usableMemory = getFreeUsableMemory();
        long freeDiskSpace = getFreeDiskSpace(directoryPath);
        if (freeDiskSpace == -1) {
            logger.error("Evaluation Error - free disk space for the directory cannot be determined : {}", directoryPath);
            return 0;
        }

        long maxRecordsBasedOnMemory = usableMemory / NUM_OF_BYTES_PER_INT_RECORD;
        long maxRecordsBasedOnDisk = freeDiskSpace / NUM_OF_BYTES_PER_INT_RECORD;

        return (long) (Math.min(maxRecordsBasedOnMemory, maxRecordsBasedOnDisk) * SAFETY_THRESHOLD);
    }

    /**
     * Calculates the optimal partition size for processing records.
     *
     * @param totalNumberOfRecords The total number of records to be processed.
     * @return The calculated partition size.
     */
    public static long calculatePartitionSize(int totalNumberOfRecords) {
        // Calculate partition size - fraction of total records
        long partitionSize = (long) (totalNumberOfRecords * PARTITION_PERCENTAGE);
        return Math.max(partitionSize, 1); // Minimum of one record per partition
    }

    private static long getFreeUsableMemory() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long allocatedMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();

        long actuallyFreeMemory = freeMemory + (maxMemory - allocatedMemory);
        return (long) (actuallyFreeMemory * SAFETY_THRESHOLD);
    }

    private static long getFreeDiskSpace(String directoryPath) {
        try {
            File file = new File(directoryPath);
            File directory = file.isDirectory() ? file : file.getParentFile();

            if (directory != null && directory.exists()) {
                return directory.getFreeSpace();
            } else {
                logger.warn("Directory does not exist for the given path: {}", directoryPath);
                return -1;
            }
        } catch (Exception e) {
            logger.error("Error occurred while getting free disk space for path '{}': {}", directoryPath, e.getMessage());
            return -1;
        }
    }
}
