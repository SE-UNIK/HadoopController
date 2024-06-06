package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.CustomMultipartFile;
import com.unik.hadoopcontroller.model.HdfsFileModel;
import com.unik.hadoopcontroller.model.SparkModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.SparkConf;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/*
* using SparkLauncher instead of SparkSubmit as SparkLauncher is a process so we can track its stdout
* only need to provide PATHs of input file(s) [from hdfs] and pyspark algorithm(s) [from spark dir], and output file [to hdfs]
* launch spark -> redirect output results to be saved in a file then stored in hdfs            OR change type to hdfsfilemodel and use post method to store to hdfs??
* -->> change output path to set to a new file create right before redirect method and pass new file path for getting output
* */


@Service
public class SparkSubmitJobService {
    //    private final SparkConf sparkConf;
//    private final SparkModel sparkModel;
    private final String hadoopRootDir = "/user/hadoop";
    private final String sparkAlgorithmsDir = hadoopRootDir + "/spark/algorithms/";
    @Autowired
    HdfsDirectService hdfsDirectService;
    //String wordCountScript = sparkAlgorithmsDir + "wordcount.py";
    //String destination = "/user/hadoop/";

    private void redirectOutput(InputStream inputStream, String filePath) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(new File(filePath));
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            fileOutputStream.write(buffer, 0, length);
        }
        fileOutputStream.close();
    }

    public File launchSparkJob(SparkModel sparkJobModel, String fileName, String filePath) {
        try {
            Process spark = new SparkLauncher()
                    .setSparkHome(hadoopRootDir + "/spark")
                    .setAppResource(sparkAlgorithmsDir + sparkJobModel.getAlgorithm())
                    .setMaster("yarn")
                    .setDeployMode("cluster")
                    .addAppArgs(hadoopRootDir + sparkJobModel.getInputPath())   // should receive parquet file
                    .setVerbose(true)
                    .launch();

            // create new file here to pass to redirectOutput path
            String analysisFileName = fileName + "_analysis_results";
            String title = fileName + "'s Analysis Results";
            String[] author = {"Sparky"};
            List<String> authors = Arrays.asList(author);

            File outputFile = new File(sparkJobModel.getOutputPath());
            MultipartFile results = new CustomMultipartFile(outputFile);

            redirectOutput(spark.getInputStream(), sparkJobModel.getOutputPath());
            // redirectOutput(spark.getErrorStream(), sparkJobModel.getOutputPath());

            int exitCode = spark.waitFor();

            hdfsDirectService.writeToHdfsUniqueWithFilePath(filePath,analysisFileName, results, title, authors);
            System.out.println("Spark job finished with exit code: " + exitCode);
            return outputFile;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.out.println("Spark job was not finished successfully.");
            return null;
        }
    }

}
