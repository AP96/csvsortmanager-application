package com.crossixanalytics.sorting.csvsortmanager.unit;

import com.crossixanalytics.sorting.csvsortmanager.service.implementations.CSVFileWriterImpl;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CSVFileWriterImplTest {
    private static final String BASE_DIR = "D:\\Documents\\Java Workspace\\csvsortmanager\\src\\test\\java\\com\\crossixanalytics\\sorting\\csvsortmanager\\unit\\";

    @Test
    public void testWriteSortedRecords() throws Exception {
        String testFilePath = BASE_DIR + "sorted_test.csv";
        List<Integer> data = Arrays.asList(5, 3, 4, 2, 1);
        CSVFileWriterImpl writer = new CSVFileWriterImpl();
        writer.writeSortedRecords(testFilePath, data);

        BufferedReader reader = new BufferedReader(new FileReader(testFilePath));
        int line = Integer.parseInt(reader.readLine());
        assertEquals(5, line);
        reader.close();
    }
}
