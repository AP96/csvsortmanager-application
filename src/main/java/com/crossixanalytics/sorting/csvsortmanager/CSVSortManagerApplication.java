package com.crossixanalytics.sorting.csvsortmanager;

import com.crossixanalytics.sorting.csvsortmanager.processor.CSVSortProcessor;
import com.crossixanalytics.sorting.csvsortmanager.util.evaluators.CommandLineEvaluator;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// -f src/main/resources/templates/input.csv -n 1000 -M
public class CSVSortManagerApplication {
    private static final Logger logger = LoggerFactory.getLogger(CSVSortManagerApplication.class);

    public static void main(String[] args) {
        Options options = CommandLineEvaluator.buildOptions();
        CommandLine cmdArgs = CommandLineEvaluator.parseArguments(args, options);
        if (cmdArgs == null) {
            logger.error("Failed to parse command-line arguments.");
            CommandLineEvaluator.printHelper(new HelpFormatter(), options);
            return;
        }

        try {
            String filePath = cmdArgs.getOptionValue("f");
            int numberOfRecords = Integer.parseInt(cmdArgs.getOptionValue("n"));
            boolean isMultiProcessing = cmdArgs.hasOption("M");
            CSVSortProcessor csvSortProcessor = new CSVSortProcessor(filePath, numberOfRecords, isMultiProcessing);
            csvSortProcessor.processCSVFile();
        } catch (NumberFormatException e) {
            logger.error("Number of records must be an integer.", e);
            CommandLineEvaluator.printHelper(new HelpFormatter(), options);
        } catch (Exception e) {
            logger.error("An unexpected error occurred: {}", e.getMessage(), e);
        }
    }
}

