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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.avro.AvroParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class SparkSubmitJobService {

    private final String sparkHome = "/home/hadoop/spark";
    private final String systemSparkAlgorithmsDir = sparkHome + "/algorithms/";
    private final String inputFilesDir = "/user/hadoop/inputs/";
    private final String venvDir = "/home/hadoop/spark/venv.tar.gz#venv";

    public static void deleteHDFSDirectory(String directoryPath) {
        Configuration configuration = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(configuration);
            Path path = new Path(directoryPath);

            if (hdfs.exists(path)) {
                hdfs.delete(path, true);
                System.out.println("Directory " + directoryPath + " deleted successfully.");
            } else {
                System.out.println("Directory " + directoryPath + " does not exist.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete HDFS directory", e);
        }
    }

    private void renameAndMoveHdfsFile(String sourcePathStr, String destPathStr) {
        Configuration configuration = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(configuration);
            Path sourcePath = new Path(sourcePathStr);
            Path destPath = new Path(destPathStr);
            Path sourceDirPath = sourcePath.getParent(); // Get the parent directory of the source file

            if (hdfs.exists(destPath)) {
                hdfs.delete(destPath, true);
            }

            boolean success = hdfs.rename(sourcePath, destPath);
            if (success) {
                System.out.println("File renamed and moved successfully.");

                // Delete the original directory
                if (hdfs.exists(sourceDirPath)) {
                    hdfs.delete(sourceDirPath, true);
                    System.out.println("Original directory deleted successfully.");
                }
            } else {
                System.out.println("File rename and move failed.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to rename and move HDFS file", e);
        }
    }


    public void launchWordcountSparkJob(List<String> fileNames) {
        Process spark = null;
        String outputPath = "/home/hadoop/wordcount_result";
        System.out.println("Starting Wordcount Spark Job");
        try {
            // Concatenate all file names into a single string separated by commas
            String inputFilePath = fileNames.stream()
                    .map(fileName -> inputFilesDir + fileName)
                    .collect(Collectors.joining(","));

            System.out.println("Launching spark job with input files: " + inputFilePath);
            spark = new SparkLauncher()
                    .setSparkHome(sparkHome)
                    .setAppResource(systemSparkAlgorithmsDir + "wordcount.py")
                    .setMaster("yarn")
                    .setDeployMode("cluster")
                    .addAppArgs(inputFilePath)
                    .setVerbose(true)
                    .launch();

            int exitCode = spark.waitFor();
            System.out.println("Spark job finished with exit code: " + exitCode);

            //deleteHDFSDirectory(outputPath);

            String renameInputFile = fileNames.stream()
                    .map(fileName -> fileName.substring(0, fileName.lastIndexOf('.'))) // Remove file extension
                    .collect(Collectors.joining("_")) + ".txt";
            String renameNewFile = "wordcount_" + renameInputFile;
            if(exitCode == 0) {
                renameAndMoveHdfsFile(outputPath + "/part-00000", renameNewFile);
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void launchKMeansSparkJob(List<String> fileNames) {
        Process spark = null;
        String outputPath = "/home/hadoop/kmeans_result";
        System.out.println("Starting KMeans Spark Job");
        try {
            String inputFilePath = fileNames.stream()
                    .map(fileName -> inputFilesDir + fileName)
                    .collect(Collectors.joining(","));

            //String args = inputFilePath + " 3 1.0";
            System.out.println("Launching spark job: " + inputFilePath);
            spark = new SparkLauncher()
                    .setSparkHome(sparkHome)
                    .setAppResource(systemSparkAlgorithmsDir + "kmeans_al.py")
                    //.addPyFile(sparkHome + "/py-files/numpy.zip")
                    .setMaster("yarn")
                    .setDeployMode("cluster")
                    .addFile(venvDir)
                    .addAppArgs(inputFilePath, "2", "0.001")
                    .setVerbose(true)
                    .launch();

            int exitCode = spark.waitFor();
            System.out.println("Spark job finished with exit code: " + exitCode);

            String renameInputFile = fileNames.stream()
                    .map(fileName -> fileName.substring(0, fileName.lastIndexOf('.'))) // Remove file extension
                    .collect(Collectors.joining("_")) + ".txt";
            String renameNewFile = "kmeans_" + renameInputFile;
            if(exitCode == 0) {
                renameAndMoveHdfsFile(outputPath + "/part-00000", renameNewFile);
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void launchTFIDFSparkJob(List<String> fileNames) {
        Process spark = null;
        String outputPath = "/home/hadoop/tfidf_result";
        System.out.println("Starting TF-IDF Spark Job");

        try {
            // Concatenate all file names into a single string separated by commas
            String inputFilePath = fileNames.stream()
                    .map(fileName -> inputFilesDir + fileName)
                    .collect(Collectors.joining(","));

            System.out.println("Launching spark job with input files: " + inputFilePath);
            spark = new SparkLauncher()
                    .setSparkHome(sparkHome)
                    .setAppResource(systemSparkAlgorithmsDir + "tfidf.py")
                    //.addPyFile(sparkHome + "/py-files/numpy.zip")
                    .setMaster("yarn")
                    .setDeployMode("cluster")
                    .addFile(venvDir)
                    .addAppArgs(inputFilePath, "20", "0.001")  // Example arguments
                    .setVerbose(true)
                    .launch();

            int exitCode = spark.waitFor();
            System.out.println("Spark job finished with exit code: " + exitCode);

            // Handle the output if the job is successful
            if (exitCode == 0) {
                String renameInputFile = fileNames.stream()
                        .map(fileName -> fileName.substring(0, fileName.lastIndexOf('.'))) // Remove file extension
                        .collect(Collectors.joining("_")) + ".txt";
                String renameNewFile = "tfidf_" + renameInputFile;
                renameAndMoveHdfsFile(outputPath + "/part-00000", renameNewFile);
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}