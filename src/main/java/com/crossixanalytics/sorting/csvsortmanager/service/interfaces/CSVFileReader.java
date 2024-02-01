package com.crossixanalytics.sorting.csvsortmanager.service.interfaces;

import java.io.IOException;
import java.util.List;

public interface CSVFileReader {
    List<Integer> readCSVRecords(String filePath, int partitionSize, long offset) throws IOException;
}
