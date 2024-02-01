package com.crossixanalytics.sorting.csvsortmanager.processor;

import com.crossixanalytics.sorting.csvsortmanager.service.implementations.*;
import com.crossixanalytics.sorting.csvsortmanager.service.interfaces.*;
import com.crossixanalytics.sorting.csvsortmanager.util.constants.IOConstants;
import com.crossixanalytics.sorting.csvsortmanager.util.evaluators.SystemSpecsEvaluator;
import com.crossixanalytics.sorting.csvsortmanager.util.validators.CSVNumberOfRecordsValidator;
import com.crossixanalytics.sorting.csvsortmanager.util.validators.FilePathValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * Processor class for sorting CSV files.
 * It supports both single-threaded and multithreaded processing modes.
 */

public class CSVSortProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CSVSortProcessor.class);
    private final String inputFilePath;
    private final int numberOfRecords;
    private final boolean multiProcessingMode;

    /**
     * Constructs a CSVSortProcessor with specified input file path, number of records, and processing mode.
     *
     * @param inputFilePath       Path to the input CSV file.
     * @param numberOfRecords     Number of records to process.
     * @param multiProcessingMode Set to true for multi-threaded processing, false for single-threaded.
     */
    public CSVSortProcessor(String inputFilePath, int numberOfRecords, boolean multiProcessingMode) {
        this.inputFilePath = inputFilePath;
        this.numberOfRecords = numberOfRecords;
        this.multiProcessingMode = multiProcessingMode;
    }

    public void processCSVFile() {
        String directoryPrefix = multiProcessingMode ? "multi-threaded-processing" : "single-threaded-processing";
        Path path = Paths.get(inputFilePath);
        Path baseDirectory = path.getParent();
        Path inputDirectory = baseDirectory.resolve(directoryPrefix + "_input");
        Path outputDirectory = baseDirectory.resolve(directoryPrefix + "_output");

        try {
            Files.createDirectories(inputDirectory);
            Files.createDirectories(outputDirectory);

            if (validateInputs()) {
                String newInputFilePath = inputDirectory.resolve(path.getFileName()).toString();
                generateCSVFile(newInputFilePath);
                List<String> sortedFilePaths = multiProcessingMode
                        ? processCSVFileMultiThreaded(newInputFilePath, outputDirectory)
                        : processCSVFileSingleThreaded(newInputFilePath, outputDirectory);
                mergeSortedFiles(sortedFilePaths, outputDirectory);
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            logger.error("Error during processing: {}", e.getMessage(), e);
        }
    }

    /**
     * Validates the input file path and number of records.
     *
     * @return true if both the file path and number of records are valid, false otherwise.
     */
    private boolean validateInputs() {
        if (!FilePathValidator.isFilePathValid(inputFilePath)) {
            logger.error("Error - Invalid file path: {}", inputFilePath);
            return false;
        }

        if (!CSVNumberOfRecordsValidator.isNumberOfRecordsInRange(numberOfRecords, inputFilePath)) {
            logger.error("Error - Invalid number of records: {}", numberOfRecords);
            return false;
        }
        logger.info("Successful Validation - Number of records {} in range and valid inputFilePath {}", numberOfRecords, inputFilePath);
        return true;
    }

    private void generateCSVFile(String newInputFilePath) throws IOException {
        CSVFileGenerator fileGenerator = new CSVFileGeneratorImpl();
        fileGenerator.createCSVFile(newInputFilePath, numberOfRecords);
        logger.info("CSV file generated successfully at path: {}", newInputFilePath);
    }

    /**
     * Processes the CSV file in single-threaded mode.
     * Reads, sorts, and writes the records in chunks defined by the partition size.
     *
     * @param newInputFilePath Path to the input file for reading.
     * @param outputDir        Path to the output directory for writing sorted files.
     * @return A list of paths to sorted file chunks.
     * @throws IOException If an I/O error occurs.
     */
    private List<String> processCSVFileSingleThreaded(String newInputFilePath, Path outputDir) throws IOException {
        long partitionSize = SystemSpecsEvaluator.calculatePartitionSize(numberOfRecords);
        long offset = 0;
        List<String> sortedFilePaths = new ArrayList<>();
        int partitionIndex = 0;

        while (partitionIndex * partitionSize < numberOfRecords) {
            List<Integer> records = new CSVFileReaderImpl().readCSVRecords(newInputFilePath, (int) partitionSize, offset);
            List<Integer> sortedRecords = new CSVSingleFileSorterImpl().sortSingleCSVFileRecords(records);
            String sortedFilePath = outputDir.resolve(IOConstants.SORTED_FILE_PREFIX + partitionIndex + IOConstants.FILE_TYPE).toString();
            new CSVFileWriterImpl().writeSortedRecords(sortedFilePath, sortedRecords);
            sortedFilePaths.add(sortedFilePath);
            offset += calculateRecordsBytes(records);
            partitionIndex++;
        }

        return sortedFilePaths;
    }


    /**
     * Processes the CSV file in multi-threaded mode using a thread pool.
     * Distributes the sorting workload across multiple threads.
     *
     * @param newInputFilePath Path to the input file for reading.
     * @param outputDir        Path to the output directory for writing sorted files.
     * @return A list of paths to sorted file chunks.
     * @throws IOException          If an I/O error occurs.
     * @throws ExecutionException   If a computation threw an exception.
     * @throws InterruptedException If the current thread was interrupted while waiting.
     */
    private List<String> processCSVFileMultiThreaded(String newInputFilePath, Path outputDir) throws IOException, ExecutionException, InterruptedException {
        ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<List<String>>> futuresList = new ArrayList<>();

        long totalRecords = countTotalNumberOfRecords(newInputFilePath);
        long partitionSize = SystemSpecsEvaluator.calculatePartitionSize(numberOfRecords);
        int partitionCount = (int) Math.ceil((double) totalRecords / partitionSize);

        List<Long> offsets = calculateOffsetsByPartition(newInputFilePath, partitionSize, partitionCount);

        for (int i = 0; i < partitionCount; i++) {
            long offset = i < offsets.size() ? offsets.get(i) : totalRecords;
            int finalI = i;
            Future<List<String>> future = threadPool.submit(() -> {
                List<Integer> records = new CSVFileReaderImpl().readCSVRecords(newInputFilePath, (int) partitionSize, offset);
                return processAndWriteSinglePartition(records, outputDir, finalI);
            });
            futuresList.add(future);
        }

        List<String> sortedFilePaths = new ArrayList<>();
        for (Future<List<String>> future : futuresList) {
            sortedFilePaths.addAll(future.get());
        }

        threadPool.shutdown();
        return sortedFilePaths;
    }


    /**
     * Processes and writes a single partition of records.
     *
     * @param records        The list of records to be sorted and written.
     * @param outputDir      The output directory for the sorted file.
     * @param partitionIndex The index of the partition.
     * @return A list containing the path to the sorted file.
     * @throws IOException If an I/O error occurs during writing.
     */
    private List<String> processAndWriteSinglePartition(List<Integer> records, Path outputDir, int partitionIndex) throws IOException {
        List<Integer> sortedRecords = new CSVSingleFileSorterImpl().sortSingleCSVFileRecords(records);
        String sortedFilePath = outputDir.resolve(IOConstants.SORTED_FILE_PREFIX + partitionIndex + IOConstants.FILE_TYPE).toString();
        new CSVFileWriterImpl().writeSortedRecords(sortedFilePath, sortedRecords);
        return Collections.singletonList(sortedFilePath);
    }

    /**
     * Calculates file offsets for each partition.
     *
     * @param filePath       Path to the file.
     * @param partitionSize  Size of each partition.
     * @param partitionCount Total number of partitions.
     * @return A list of offsets for each partition.
     * @throws IOException If an I/O error occurs.
     */
    private List<Long> calculateOffsetsByPartition(String filePath, long partitionSize, int partitionCount) throws IOException {
        List<Long> offsets = new ArrayList<>();
        long currentOffset = 0;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            for (int i = 0; i < partitionCount; i++) {
                offsets.add(currentOffset);
                file.seek(currentOffset);

                long lineCount = 0;
                while (lineCount < partitionSize && file.readLine() != null) {
                    lineCount++;
                    currentOffset = file.getFilePointer();
                }
            }
        }
        return offsets;
    }

    private long countTotalNumberOfRecords(String newInputFilePath) throws IOException {
        long recordCount = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(newInputFilePath))) {
            while (reader.readLine() != null) {
                recordCount++;
            }
        }
        return recordCount;
    }

    /**
     * Calculates the total bytes of all records in a list.
     *
     * @param records The list of records.
     * @return The total bytes of all records.
     */
    private long calculateRecordsBytes(List<Integer> records) {
        return records.stream().mapToInt(num -> Integer.toString(num).length() + System.lineSeparator().length()).sum();
    }

    /**
     * Merges sorted files into a single final sorted file.
     *
     * @param sortedFilePaths List of paths to sorted file chunks.
     * @param outputDir       Path to the output directory for the final merged file.
     * @throws IOException          If an I/O error occurs.
     * @throws InterruptedException If the current thread was interrupted while waiting.
     * @throws ExecutionException   If a computation threw an exception.
     */
    private void mergeSortedFiles(List<String> sortedFilePaths, Path outputDir) throws IOException, InterruptedException, ExecutionException {
        if (sortedFilePaths.size() <= 1) {
            if (!sortedFilePaths.isEmpty()) {
                Files.move(Paths.get(sortedFilePaths.get(0)), outputDir.resolve(IOConstants.FINAL_SORTED_FILENAME));
            }
            return;
        }

        String finalOutputFilePath = outputDir.resolve(IOConstants.FINAL_SORTED_FILENAME).toString();
        parallelMerge(sortedFilePaths, finalOutputFilePath);
    }

    /**
     * Parallel merging of sorted file chunks.
     *
     * @param sortedFilePaths     List of sorted file paths.
     * @param finalOutputFilePath Path for the final output file.
     * @return A list of temporary file paths used during the merge.
     * @throws InterruptedException If the current thread was interrupted while waiting.
     * @throws ExecutionException   If a computation threw an exception.
     * @throws IOException          If an I/O error occurs.
     */
    private void parallelMerge(List<String> sortedFilePaths, String finalOutputFilePath) throws InterruptedException, ExecutionException, IOException {
        List<Path> tempFiles = executeParallelMerging(sortedFilePaths, finalOutputFilePath);
        cleanUpTemporaryFiles(tempFiles);
    }

    private List<Path> executeParallelMerging(List<String> sortedFilePaths, String finalOutputFilePath) throws InterruptedException, ExecutionException, IOException {
        ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<String>> mergeFutures = new ArrayList<>();
        List<Path> tempMergeFiles = new ArrayList<>();

        int groupSize = 2;
        for (int i = 0; i < sortedFilePaths.size(); i += groupSize) {
            List<String> sortedFilePathsGroup = sortedFilePaths.subList(i, Math.min(i + groupSize, sortedFilePaths.size()));
            mergeFutures.add(threadPool.submit(() -> mergeFilesAndTrackTempFile(sortedFilePathsGroup, tempMergeFiles)));
        }

        List<String> intermediateMergedFiles = new ArrayList<>();
        for (Future<String> future : mergeFutures) {
            intermediateMergedFiles.add(future.get());
        }

        if (!intermediateMergedFiles.isEmpty()) {
            new CSVFileMergerImpl().mergeCSVFiles(intermediateMergedFiles, finalOutputFilePath);
        }

        threadPool.shutdown();
        return tempMergeFiles;
    }

    /**
     * Merges a group of sorted files and tracks the temporary file.
     *
     * @param group     List of sorted file paths to be merged.
     * @param tempFiles List to track temporary files.
     * @return The path of the merged file.
     * @throws IOException If an I/O error occurs.
     */
    private String mergeFilesAndTrackTempFile(List<String> group, List<Path> tempFiles) throws IOException {
        CSVFileMerger fileMerger = new CSVFileMergerImpl();
        String mergedFilePath = IOConstants.TEMP_FILE_PREFIX + System.nanoTime() + IOConstants.FILE_TYPE;
        fileMerger.mergeCSVFiles(group, mergedFilePath);
        tempFiles.add(Paths.get(mergedFilePath));
        return mergedFilePath;
    }

    /**
     * Cleans up temporary files used during the merging process.
     *
     * @param tempFiles List of paths to the temporary files.
     */
    private void cleanUpTemporaryFiles(List<Path> tempFiles) {
        for (Path tempFile : tempFiles) {
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException e) {
                logger.error("Error deleting temporary file: " + tempFile, e);
            }
        }
    }
}
