package com.crossixanalytics.sorting.csvsortmanager.service.implementations;

import com.crossixanalytics.sorting.csvsortmanager.service.interfaces.CSVFileReader;
import com.crossixanalytics.sorting.csvsortmanager.util.evaluators.CommandLineEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class CSVFileReaderImpl implements CSVFileReader {
    private static final Logger logger = LoggerFactory.getLogger(CSVFileReaderImpl.class);

    /**
     * Reads a specific number of records from a CSV file starting from a given offset.
     *
     * @param filePath The path of the CSV file to read from.
     * @param partitionSize The number of records to read.
     * @param offset The offset to start reading from in the file.
     * @return A list of integer records read from the file.
     * @throws IOException If an I/O error occurs.
     */
    public List<Integer> readCSVRecords(String filePath, int partitionSize, long offset) throws IOException {
        List<Integer> csvRecords = new ArrayList<>();
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            file.seek(offset); // Move to the specific offset
            String line;
            int count = 0;
            while ((line = file.readLine()) != null && count < partitionSize) {
                line = line.trim();
                if (!line.isEmpty()) {
                    try {
                        csvRecords.add(Integer.parseInt(line));
                        count++;
                    } catch (NumberFormatException e) {
                        logger.warn("NumberFormatException Parsing exception occurred: '{}'", line, e);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("IO Exception occurred while reading CSV records", e);
            throw e;
        }
        return csvRecords;
    }
}
