package com.crossixanalytics.sorting.csvsortmanager.util.validators;

import com.crossixanalytics.sorting.csvsortmanager.util.evaluators.SystemSpecsEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVNumberOfRecordsValidator {
    private static final int MIN_RECORDS = 50;
    private static final int MAX_RECORDS = 10000000;

    private static final Logger logger = LoggerFactory.getLogger(CSVNumberOfRecordsValidator.class);

    public static boolean isNumberOfRecordsInRange(int numberOfRecords, String inputDirPath) {
        if (numberOfRecords < MIN_RECORDS || numberOfRecords > MAX_RECORDS) {
            logger.warn("Illegal Number of records {} given out of the allowed range ({}, {}).", numberOfRecords, MIN_RECORDS, MAX_RECORDS);
            return false;
        }
        try {
            long calculatedMaxRecords = SystemSpecsEvaluator.calculateMaxRecords(inputDirPath);
            if (numberOfRecords > calculatedMaxRecords) {
                logger.warn("Number of records {} exceeds the system's capacity of {} based on directory '{}'.", numberOfRecords, calculatedMaxRecords, inputDirPath);
                return false;
            }
            return true;
        } catch (Exception e) {
            logger.error("Error calculating maximum records for the directory '{}': {}", inputDirPath, e.getMessage());
            return false;
        }

    }
}
