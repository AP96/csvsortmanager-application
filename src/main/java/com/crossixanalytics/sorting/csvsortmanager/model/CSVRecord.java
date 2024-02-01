package com.crossixanalytics.sorting.csvsortmanager.model;

import java.io.BufferedReader;

/**
 * Represents a single record from a CSV file.
 * This class implements the Comparable interface for sorting based on the CSV record's value.
 */
public class CSVRecord implements Comparable<CSVRecord> {
    private final int recordValue;
    private final BufferedReader bufferedReader;

    public CSVRecord(int recordValue, BufferedReader bufferedReader) {
        this.recordValue = recordValue;
        this.bufferedReader = bufferedReader;

    }

    @Override
    public int compareTo(CSVRecord csvRecord) {
        return Integer.compare(this.recordValue, csvRecord.recordValue);
    }

    public BufferedReader getBufferedReader() {
        return bufferedReader;
    }

    public int getRecordValue() {
        return recordValue;
    }

}
