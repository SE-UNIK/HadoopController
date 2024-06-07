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

@Service
public class SparkSubmitJobService {

    private final String systemHadoopRootDir = "/home/hadoop";
    private final String systemSparkAlgorithmsDir = systemHadoopRootDir + "/spark/algorithms/";
    private final String hdfsRootDir = "/user/hadoop/";

    @Autowired
    private HdfsDirectService hdfsDirectService;

    @Autowired
    private SparkModel sparkModel;

    private void redirectOutput(InputStream inputStream, OutputStream outputStream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    public File launchSparkJob(SparkModel sparkJobModel, String fileName) {
        Process spark = null;
        File tempOutputFile = null;
        try {
            String inputFilePath = hdfsRootDir + sparkJobModel.getInputDirectoryPath() + sparkJobModel.getInputFileName();

            spark = new SparkLauncher()
                    .setSparkHome(systemHadoopRootDir + "/spark")
                    .setAppResource(systemSparkAlgorithmsDir + sparkJobModel.getAlgorithmName())
                    .setMaster("yarn")
                    .setDeployMode("cluster")
                    .addAppArgs(inputFilePath)
                    .setVerbose(true)
                    .launch();

            String analysisFileName = "analysis_results_" + fileName;
            String title = fileName + "'s Analysis Results";
            List<String> authors = Arrays.asList("Sparky");
            String systemOutputFilePath = "output/" + analysisFileName;

            if (!Files.exists(Paths.get("output"))) {
                Files.createDirectories(Paths.get("output"));
            }

            tempOutputFile = new File(systemOutputFilePath);

            try (FileOutputStream fileOutputStream = new FileOutputStream(tempOutputFile)) {
                // Redirect both stdout and stderr to the file
                redirectOutput(spark.getInputStream(), fileOutputStream);
                redirectOutput(spark.getErrorStream(), fileOutputStream);
            }

            int exitCode = spark.waitFor();
            System.out.println("Spark job finished with exit code: " + exitCode);

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
