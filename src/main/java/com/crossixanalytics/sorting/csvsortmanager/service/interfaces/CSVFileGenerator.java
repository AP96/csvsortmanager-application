package com.crossixanalytics.sorting.csvsortmanager.service.interfaces;

import java.io.IOException;

/**
 * Responsible for generating a CSV file based on a specific number of integer records
 */
public interface CSVFileGenerator {

    void createCSVFile(String fileDestinationPath, int numberOfRecords) throws IOException;
}
