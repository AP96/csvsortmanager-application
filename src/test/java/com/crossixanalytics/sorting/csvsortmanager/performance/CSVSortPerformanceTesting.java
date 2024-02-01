package com.crossixanalytics.sorting.csvsortmanager.performance;

import com.crossixanalytics.sorting.csvsortmanager.processor.CSVSortProcessor;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class CSVSortPerformanceTesting {

    @Test
    public void testSortingPerformance() {
        String filePath = "D:\\Documents\\Java Workspace\\csvsortmanager\\src\\test\\java\\com\\crossixanalytics\\sorting\\csvsortmanager\\performance\\test.csv";
        int numberOfRecords = 5000;

        CSVSortProcessor singleThreadProcessor = new CSVSortProcessor(filePath, numberOfRecords, false);
        CSVSortProcessor multiThreadProcessor = new CSVSortProcessor(filePath, numberOfRecords, true);

        long startTime = System.currentTimeMillis();
        singleThreadProcessor.processCSVFile();
        long singleThreadProcessingDeltaTime = System.currentTimeMillis() - startTime;

        startTime = System.currentTimeMillis();
        multiThreadProcessor.processCSVFile();
        long multiThreadProcessingDeltaTime = System.currentTimeMillis() - startTime;

        assertTrue(multiThreadProcessingDeltaTime < singleThreadProcessingDeltaTime); // We expect multi-threaded operation to perform faster
    }

}
