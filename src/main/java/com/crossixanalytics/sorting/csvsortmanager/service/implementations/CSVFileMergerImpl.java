package com.crossixanalytics.sorting.csvsortmanager.service.implementations;

import com.crossixanalytics.sorting.csvsortmanager.model.CSVRecord;
import com.crossixanalytics.sorting.csvsortmanager.service.interfaces.CSVFileMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class CSVFileMergerImpl implements CSVFileMerger {
    private static final Logger logger = LoggerFactory.getLogger(CSVFileMergerImpl.class);

    /**
     * Merge a list of sorted CSV files to a single sorted CSV file.
     *
     * @param sortedFiles List of paths to the sorted CSV files.
     * @param outputFilePath Path for the output merged CSV file.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void mergeCSVFiles(List<String> sortedFiles, String outputFilePath) throws IOException {
        PriorityQueue<CSVRecord> minimumHeap = new PriorityQueue<>();
        List<BufferedReader> bufferedReaders = new ArrayList<>(); // Track open readers for a smooth closing operation process

        for (String file : sortedFiles) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(file));
                bufferedReaders.add(reader); // Add to list to ensure closure later
                String readLine = reader.readLine();
                if (readLine != null) {
                    minimumHeap.add(new CSVRecord(Integer.parseInt(readLine.trim()), reader));
                }
            } catch (FileNotFoundException e) {
                logger.error("FileNotFoundException : {}", file, e);
            } catch (IOException | NumberFormatException e) {
                logger.error("Exception while processing file: {}", file, e);
            }
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            while (!minimumHeap.isEmpty()) {
                CSVRecord record = minimumHeap.poll();
                writer.write(record.getRecordValue() + "\n");
                readNextCSVRecord(record, minimumHeap);
            }
        } finally {
            closeAllBufferedReaders(bufferedReaders);
        }
    }

    private void readNextCSVRecord(CSVRecord record, PriorityQueue<CSVRecord> minimumHeap) {
        try {
            String nextLine = record.getBufferedReader().readLine();
            if (nextLine != null && !nextLine.trim().isEmpty()) {
                minimumHeap.add(new CSVRecord(Integer.parseInt(nextLine.trim()), record.getBufferedReader()));
            }
        } catch (IOException | NumberFormatException e) {
            logger.error("Exception while reading next line", e);
        }
    }

    private void closeAllBufferedReaders(List<BufferedReader> readers) {
        for (BufferedReader reader : readers) {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                logger.error("Exception while closing file reader", e);
            }
        }
    }
}
