package com.crossixanalytics.sorting.csvsortmanager.unit;

import com.crossixanalytics.sorting.csvsortmanager.processor.CSVSortProcessor;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class CSVSortProcessorTest {
    private static final String BASE_DIR = "D:\\Documents\\Java Workspace\\csvsortmanager\\src\\test\\java\\com\\crossixanalytics\\sorting\\csvsortmanager\\unit\\";

    @Test
    public void testProcessCSVFileSingleThreaded() throws Exception {
        String inputFilePath = BASE_DIR + "test.csv";
        CSVSortProcessor processor = new CSVSortProcessor(inputFilePath, 100, false);
        processor.processCSVFile();

        Path sortedFilePath = Paths.get(BASE_DIR + "single-threaded-processing_output\\final_sorted.csv");
        assertTrue("Sorted file does not exist", Files.exists(sortedFilePath));

        verifySortedFileContents(sortedFilePath.toString(), 100);
    }

    @Test
    public void testProcessCSVFileMultiThreaded() throws Exception {
        String inputFilePath = BASE_DIR + "test.csv";
        CSVSortProcessor processor = new CSVSortProcessor(inputFilePath, 100, true);
        processor.processCSVFile();

        Path sortedFilePath = Paths.get(BASE_DIR + "multi-threaded-processing_output\\final_sorted.csv");
        assertTrue("Sorted file does not exist", Files.exists(sortedFilePath));

        verifySortedFileContents(sortedFilePath.toString(), 100);
    }

    private void verifySortedFileContents(String filePath, int expectedRecordCount) throws Exception {
        List<Integer> records = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                records.add(Integer.parseInt(line.trim()));
            }
        }

        assertEquals("Incorrect number of records", expectedRecordCount, records.size());

        for (int i = 1; i < records.size(); i++) {
            assertTrue("Records are not sorted properly", records.get(i) >= records.get(i - 1));
        }
    }
}
