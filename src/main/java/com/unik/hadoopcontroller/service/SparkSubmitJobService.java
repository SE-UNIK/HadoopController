package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.CustomMultipartFile;
import com.unik.hadoopcontroller.model.SparkModel;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class SparkSubmitJobService {

    private final String systemHadoopRootDir = "/home/hadoop";
    private final String systemSparkAlgorithmsDir = systemHadoopRootDir + "/spark/algorithms/";
    private final String hdfsRootDir = "/user/hadoop/";

    @Autowired
    private HdfsDirectService hdfsDirectService;

    @Autowired
    private SparkModel sparkModel;

//    private void redirectOutput(InputStream inputStream, OutputStream outputStream) {
//        System.out.print(" getting log file");
//        Executors.newSingleThreadExecutor().submit(() -> {
//            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
//                String line;
//                boolean isEmpty = true;
//                while ((line = reader.readLine()) != null) {
//                    writer.write(line);
//                    System.out.println(line);
//                    writer.newLine();
//                    if (!line.trim().isEmpty()) {
//                        isEmpty = false;
//                    }
//                }
//
//                if (isEmpty) {
//                    System.out.println("Input file is empty.");
//                    // Handle empty file scenario here (e.g., log a message, throw an exception)
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//    }



    private void redirectOutput(InputStream inputStream, OutputStream outputStream) {
        System.out.println("Getting log file");
        Thread thread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
                String line;
                boolean isEmpty = true;
                while ((line = reader.readLine()) != null) {
                    writer.write(line);
                    System.out.println(line);
                    writer.newLine();
                    if (!line.trim().isEmpty()) {
                        isEmpty = false;
                    }
                }

                if (isEmpty) {
                    System.out.println("Input file is empty.");
                    // Handle empty file scenario here (e.g., log a message, throw an exception)
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thread.start();

        try {
            thread.join(); // Wait for the thread to finish
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
//            String line;
//            String containerId = System.getenv("CONTAINER_ID");
//            String containerWorkDir = System.getenv("PWD");
//
//            System.out.println("Container ID: " + containerId);
//            System.out.println("Container Working Directory: " + containerWorkDir);
//
//            System.out.println("Writing log file");
//            while ((line = reader.readLine()) != null) {
//                // Process the line here
//                System.out.println(line); // Example: Print the line to the console
//            }
//        } catch (IOException e) {
//            e.printStackTrace(); // Handle any IO exceptions
//        }

    }


    public File launchSparkJob(SparkModel sparkJobModel, String fileName) {
        Process spark = null;
        File tempOutputFile = null;
        System.out.println("Starting spark job");
        try {
            String inputFilePath = hdfsRootDir + sparkJobModel.getInputDirectoryPath() + sparkJobModel.getInputFileName();

            System.out.println("Launching spark job: " + inputFilePath);
            spark = new SparkLauncher()
                    .setSparkHome(systemHadoopRootDir + "/spark")
                    .setAppResource(systemSparkAlgorithmsDir + sparkJobModel.getAlgorithmName())
                    .setMaster("yarn")
                    .setDeployMode("cluster")
                    .addAppArgs(inputFilePath)
                    .setVerbose(true)
                    .launch();

            String containerId = System.getenv("CONTAINER_ID");
            String containerWorkDir = System.getenv("PWD");

            System.out.println("Container ID: " + containerId);
            System.out.println("Container Working Directory: " + containerWorkDir);

            String analysisFileName = "analysis_results_" + fileName;
            String title = fileName + "'s Analysis Results";
            List<String> authors = Arrays.asList("Sparky");
            String systemOutputFilePath = "output/" + analysisFileName;

            if (!Files.exists(Paths.get("output"))) {
                Files.createDirectories(Paths.get("output"));
            }

            tempOutputFile = new File(systemOutputFilePath);
            File stdoutFile = new File("output/stdout.log");
            File stderrFile = new File("output/stderr.log");

            try (FileOutputStream stdoutStream = new FileOutputStream(stdoutFile);
                 FileOutputStream stderrStream = new FileOutputStream(stderrFile)) {
                // Redirect both stdout and stderr to separate files
                System.out.println("Executing spark job: " + inputFilePath);

                if (spark.getInputStream() != null) {
                    // Input stream is not null, proceed with redirection
                    redirectOutput(spark.getInputStream(), stdoutStream);
                    redirectOutput(spark.getErrorStream(), stderrStream);
                } else {
                    // Input stream is null, handle the situation accordingly
                    System.err.println("Input stream is null. Unable to redirect output.");
                }
//                redirectOutput(spark.getInputStream(), stdoutStream);
//                redirectOutput(spark.getErrorStream(), stderrStream);
            }

            int exitCode = spark.waitFor();
            System.out.println("Spark job finished with exit code: " + exitCode);

            // Combine stdout and stderr into a single file
            try (FileOutputStream outputStream = new FileOutputStream(tempOutputFile, true);
                 FileInputStream stdoutInputStream = new FileInputStream(stdoutFile);
                 FileInputStream stderrInputStream = new FileInputStream(stderrFile)) {
                byte[] buffer = new byte[1024];
                int length;
                while ((length = stdoutInputStream.read(buffer)) != -1) {
                    System.out.println("Length of file: " + length);
                    outputStream.write(buffer, 0, length);
                }
                while ((length = stderrInputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, length);
                }
            }

            // Read the temporary output file and write it to HDFS
            MultipartFile results = new CustomMultipartFile(tempOutputFile);
            hdfsDirectService.writeToHdfsUniqueWithFilePath(sparkJobModel.getOutputDirectoryPath(), analysisFileName, results, title, authors);

            return tempOutputFile;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.out.println("Spark job was not finished successfully.");
            return null;
        } finally {
            if (spark != null) {
                spark.destroy();
            }
            if (tempOutputFile != null && tempOutputFile.exists()) {
                tempOutputFile.delete();
            }
        }
    }
}
