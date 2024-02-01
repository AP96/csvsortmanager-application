package com.crossixanalytics.sorting.csvsortmanager.service.interfaces;

import java.io.IOException;
import java.util.List;

public interface CSVFileWriter {
    void writeSortedRecords(String filePath, List<Integer> records) throws IOException;
}
