CSVSortManagerApplication  

The CSVSortManagerApplication is a Java-based application for sorting CSV files. 
Supports both single-threaded and multithreaded modes, allowing efficient handling of large datasets. 
This document provides all the instructions for running the application.

Prerequisites

    - Java JDK 1.8 or higher
    - Apache Maven 3.x

Installing and Running

Follow these steps to get a development environment running:

(1) Clone Repository:
    Clone the CSVSortManagerApplication repository to your local machine.

(2) Open Terminal:
    Access the terminal in your IDE or open a command prompt.

(3) CD to Project Root:
    Use your terminal to get to the root directory of the project.

--------------------------------------------------------------------------------------

A)  Terminal - By Creating a Fat JAR:
    
1.1) Run this Maven command to build and compile the project.
    You can create a "fat" JAR which includes all dependencies. 
    This makes running the application easier because you only need to manage one JAR file:

    mvn clean compile assembly:single


1.2) Run the application for example with these input parameters:

java -jar target/csvsortmanager-0.0.1-SNAPSHOT-jar-with-dependencies.jar -f "src/main/resources/templates/CSVInputFile.csv" -n 100000

"src/main/resources/templates/CSVInputFile.csv" -> can be replaced with your chosen source directory for the creation of input csv file
100000 -> can be replaced with the number of csv records of your choice (According to program's predefined min and max limits)
Note: Adjust your Maven POM file to include the maven-assembly-plugin if necessary.

--------------------------------------------------------------------------------------

B) CommandLine - Configured Arguments

Command-Line Arguments

When running the CSVSortManagerApplication, you need to provide the following arguments:

    -f or --fileInputPath: Path to the input CSV file. (required)
    -n or --numberOfRecords: Number of records to process in the CSV file. (required)
    -M or --multiProcessing: Enable multi-threaded processing (optional).

For example in Run/Debug configurations under CSVSortManagerApplication add the following CLI Arguments:

-f src/main/resources/templates/input.csv -n 1000 -M