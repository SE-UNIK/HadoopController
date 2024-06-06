package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.model.SparkModel;
import org.springframework.stereotype.Service;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.SparkConf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Service
public class SparkSubmitJobService {
//    private final SparkConf sparkConf;
//    private final SparkModel sparkModel;
    String hadoopRootDir = "/user/hadoop";
    String sparkAlgorithmsDir = hadoopRootDir + "/spark/algorithms";
    String wordCountScript = sparkAlgorithmsDir + "/wordcount.py";
    String destination = "/user/hadoop/";

    private void redirectOutput(InputStream inputStream, String filePath) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(new File(filePath));
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            fileOutputStream.write(buffer, 0, length);
        }
        fileOutputStream.close();
    }

    public void launchSparkJob(SparkModel sparkJobModel) {
        try {
            Process spark = new SparkLauncher()
                    .setAppResource(sparkJobModel.getAlgorithm())
                    .setMaster("yarn")
                    .setDeployMode("cluster")
                    .addAppArgs(sparkJobModel.getInputPath())
                    .setVerbose(true)
                    .launch();

            redirectOutput(spark.getInputStream(), sparkJobModel.getOutputPath() + "/spark_job_output.log");
            redirectOutput(spark.getErrorStream(), sparkJobModel.getOutputPath() + "/spark_job_error.log");

            int exitCode = spark.waitFor();
            System.out.println("Spark job finished with exit code: " + exitCode);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

//    public String buildSparkCommand() {
//        // Extract attributes from SparkModel
//        String inputPath = sparkModel.getInputPath();
//        String outputPath = sparkModel.getOutputPath();
//        List<String> algorithms = sparkModel.getAlgorithm();
//
//        // Construct Spark command based on user inputs
//        StringBuilder commandBuilder = new StringBuilder("spark-submit");
//        commandBuilder.append(" --master ").append(sparkConf.get("spark.master"));
//        commandBuilder.append(" --deploy-mode ").append(sparkConf.get("spark.deploy.mode"));
//        commandBuilder.append(sparkConf.get("algorithm"));
////        for (String algorithm : algorithms) {
////            commandBuilder.append(" ").append(algorithm);
////        }
//        commandBuilder.append(" ").append(inputPath).append(" ").append(outputPath);
//
//        return commandBuilder.toString();
//    }

//    public void submitSparkJob() {
//
//    }


}
