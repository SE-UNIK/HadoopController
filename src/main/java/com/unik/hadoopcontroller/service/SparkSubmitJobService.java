package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.CustomMultipartFile;
import com.unik.hadoopcontroller.model.SparkModel;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

    private void redirectOutput(InputStream inputStream, File outputFile) throws IOException {
        try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = inputStream.read(buffer)) != -1) {
                fileOutputStream.write(buffer, 0, length);
            }
        }
    }

    public File launchSparkJob(SparkModel sparkJobModel, String fileName) {
        Process spark = null;
        File tempOutputFile = null;
        try {
            String inputFilePath = hdfsRootDir + sparkJobModel.getInputDirectoryPath() + sparkJobModel.getInputFileName();
            File stdoutFile = new File("output/stdout.log");

            File stderrFile = new File("output/stderr.log");
            spark = new SparkLauncher()
                    .setSparkHome(systemHadoopRootDir + "/spark")
                    .setAppResource(systemSparkAlgorithmsDir + sparkJobModel.getAlgorithmName())
                    .setMaster("yarn")
                    .setDeployMode("cluster")
                    .addAppArgs(inputFilePath)
                    .setVerbose(true)
                    .redirectOutput(stdoutFile)  // Redirect stdout
                    .redirectError(stderrFile)   // Redirect stderr
                    .launch();

            String analysisFileName = "analysis_results_" + fileName;
            String title = fileName + "'s Analysis Results";
            List<String> authors = Arrays.asList("Sparky");
            String systemOutputFilePath = "output/" + analysisFileName;

            if (!Files.exists(Paths.get("output"))) {
                Files.createDirectories(Paths.get("output"));
            }

            // Wait for the process to finish and capture the output
            int exitCode = spark.waitFor();
            System.out.println("Spark job finished with exit code: " + exitCode);

            // Combine stdout and stderr into a single file
            tempOutputFile = new File(systemOutputFilePath);
            try (FileOutputStream outputStream = new FileOutputStream(tempOutputFile)) {
                Files.copy(Paths.get("output/stdout.log"), outputStream);
                Files.copy(Paths.get("output/stderr.log"), outputStream);
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
