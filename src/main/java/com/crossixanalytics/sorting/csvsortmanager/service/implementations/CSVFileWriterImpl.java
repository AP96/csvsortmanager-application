package com.crossixanalytics.sorting.csvsortmanager.service.implementations;

import com.crossixanalytics.sorting.csvsortmanager.service.interfaces.CSVFileWriter;
import com.crossixanalytics.sorting.csvsortmanager.util.constants.IOConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CSVFileWriterImpl implements CSVFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(CSVFileWriterImpl.class);

    /**
     * Writes a list of sorted integer records to a CSV file.
     *
     * @param filePath The path of the CSV file to write to.
     * @param records The list of sorted integer records to write.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void writeSortedRecords(String filePath, List<Integer> records) throws IOException {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath), IOConstants.BUFFER_SIZE)) {
            int size = records.size();
            for (int i = 0; i < records.size() - 1; i++) {
                bufferedWriter.write(records.get(i).toString());
                bufferedWriter.newLine();
            }
            bufferedWriter.write(records.get(size - 1).toString());
        } catch (IOException e) {
            logger.error("IO Exception Error while writing to file: {}", filePath, e);
            throw e;
        }
    }

}
