package com.crossixanalytics.sorting.csvsortmanager.service.implementations;

import com.crossixanalytics.sorting.csvsortmanager.service.interfaces.CSVFileGenerator;
import com.crossixanalytics.sorting.csvsortmanager.util.constants.IOConstants;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class CSVFileGeneratorImpl implements CSVFileGenerator {
    /**
     * Generates a CSV file at the specific path with an input number of random records.
     *
     * @param fileDestinationPath The path where the CSV file will be created.
     * @param numberOfRecords The number of records to generate in the CSV file.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void createCSVFile(String fileDestinationPath, int numberOfRecords) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileDestinationPath), IOConstants.BUFFER_SIZE)) {
            Random generatedRandom = new Random();
            for (int i = 0; i < numberOfRecords - 1; i++) {
                bw.write(Integer.toString(generatedRandom.nextInt()));
                bw.newLine();
            }
            bw.write(Integer.toString(generatedRandom.nextInt()));

        }

    }
}
