package com.crossixanalytics.sorting.csvsortmanager.util.evaluators;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Utility class for evaluating and parsing the input command-line arguments.
 */
public class CommandLineEvaluator {
    private static final Logger logger = LoggerFactory.getLogger(CommandLineEvaluator.class);

    public static CommandLine parseArguments(String[] args , Options options) {
        CommandLineParser commandLineParser = new DefaultParser();

        try {
            return commandLineParser.parse(options, args);
        } catch (ParseException e) {
            logger.error("Parsing failed - Error message details : {}", e.getMessage());
            return null; // To mark that parsing failed
        }
    }

    public static Options buildOptions() {
        Options options = new Options();

        Option fileInputPathOption = Option.builder("f")
                .longOpt("fileInputPath")
                .hasArg()
                .desc("Original CSV file Path Input")
                .required(true)
                .build();
        options.addOption(fileInputPathOption);

        Option numberOfRecordsOption = Option.builder("n")
                .longOpt("numberOfRecords")
                .hasArg()
                .desc("Number of CSV Records to generate")
                .required(true)
                .build();
        options.addOption(numberOfRecordsOption);

        Option multiProcessingOption = Option.builder("M")
                .longOpt("multiProcessing")
                .desc("Option to Allow Multi-Threaded processing")
                .build();
        options.addOption(multiProcessingOption);

        return options;
    }

    public static void printHelper(HelpFormatter formatter, Options options) {
        String header = "CSV Sort Manager Application\n\n";
        String footer = "\nPlease provide the file path and the number of records as command-line arguments.";
        formatter.printHelp("CSVSortManagerApplication", header, options, footer, true);
    }
}
