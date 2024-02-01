package com.crossixanalytics.sorting.csvsortmanager.service.interfaces;

import java.io.IOException;
import java.util.List;

public interface CSVFileMerger {
    void mergeCSVFiles(List<String> sortedFiles, String outputFilePath) throws IOException;
}
