package com.crossixanalytics.sorting.csvsortmanager.util.validators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility class for validating file paths.
 */
public class FilePathValidator {
    private static final Logger logger = LoggerFactory.getLogger(FilePathValidator.class);

    public static boolean isFilePathValid(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            logger.warn("Given input file path is empty or null. Please check again.");
            return false;
        }

        Path fileDestinationPath = Paths.get(filePath).normalize();

        if (fileDestinationPath.getParent() == null) {
            logger.warn("Given input file path '{}' has no parent directory. Please check again.", filePath);
            return false;
        }

        if (!Files.exists(fileDestinationPath.getParent())) {
            logger.warn("Parent directory of '{}' does not exist. Please check again.", filePath);
            return false;
        }

        if (!Files.isDirectory(fileDestinationPath.getParent())) {
            logger.warn("Parent path of '{}' is not a directory. Please check again.", filePath);
            return false;
        }

        return isDestDirectoryWritable(fileDestinationPath.getParent());
    }

    private static boolean isDestDirectoryWritable(Path destinationDirectory) {
        try {
            Path testFile = Files.createTempFile(destinationDirectory, null, null);
            Files.deleteIfExists(testFile);
            return true;
        } catch (IOException e) {
            logger.error("An error occurred while checking if directory '{}' is writable: {}", destinationDirectory, e.getMessage());
            return false;
        }
    }
}
