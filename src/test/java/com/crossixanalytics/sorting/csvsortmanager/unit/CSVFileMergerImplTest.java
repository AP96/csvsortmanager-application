package com.crossixanalytics.sorting.csvsortmanager.unit;

import com.crossixanalytics.sorting.csvsortmanager.service.implementations.CSVFileMergerImpl;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class CSVFileMergerImplTest {
    // First Run CSVSortProcessorTest in order to test this part
    private static final String BASE_DIR = "D:\\Documents\\Java Workspace\\csvsortmanager\\src\\test\\java\\com\\crossixanalytics\\sorting\\csvsortmanager\\unit\\single-threaded-processing_output";

    @Test
    public void testMergeCSVFiles() throws Exception {
        List<String> sortedFiles = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            String fileName = "sorted_file_part_" + i + ".csv";
            Path filePath = Paths.get(BASE_DIR, fileName);
            assertTrue("File not found: " + filePath, Files.exists(filePath));
            sortedFiles.add(filePath.toString());
        }

        String mergedFilePath = BASE_DIR + "final_csv_sorted.csv";
        CSVFileMergerImpl merger = new CSVFileMergerImpl();
        merger.mergeCSVFiles(sortedFiles, mergedFilePath);

        assertTrue(Files.exists(Paths.get(mergedFilePath)));
    }
}
