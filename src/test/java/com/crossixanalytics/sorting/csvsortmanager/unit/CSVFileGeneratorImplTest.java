package com.crossixanalytics.sorting.csvsortmanager.unit;

import com.crossixanalytics.sorting.csvsortmanager.service.implementations.CSVFileGeneratorImpl;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CSVFileGeneratorImplTest {
    private static final String BASE_DIR = "D:\\Documents\\Java Workspace\\csvsortmanager\\src\\test\\java\\com\\crossixanalytics\\sorting\\csvsortmanager\\unit\\";

    @Test
    public void testCreateCSVFile() throws Exception {
        String testFilePath = BASE_DIR + "test.csv";
        int numberOfRecords = 100;
        CSVFileGeneratorImpl generator = new CSVFileGeneratorImpl();
        generator.createCSVFile(testFilePath, numberOfRecords);

        File file = new File(testFilePath);
        assertTrue(file.exists());

        BufferedReader reader = new BufferedReader(new FileReader(file));
        int linesCount = 0;
        while (reader.readLine() != null) {
            linesCount++;
        }
        reader.close();

        assertEquals(numberOfRecords, linesCount);
    }
}
