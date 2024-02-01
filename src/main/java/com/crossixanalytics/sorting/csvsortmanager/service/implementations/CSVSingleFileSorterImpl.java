package com.crossixanalytics.sorting.csvsortmanager.service.implementations;

import com.crossixanalytics.sorting.csvsortmanager.service.interfaces.CSVSingleFileSorter;

import java.util.Collections;
import java.util.List;

public class CSVSingleFileSorterImpl implements CSVSingleFileSorter {
    @Override
    public List<Integer> sortSingleCSVFileRecords(List<Integer> records) {
        Collections.sort(records);
        return records;
    }
}
