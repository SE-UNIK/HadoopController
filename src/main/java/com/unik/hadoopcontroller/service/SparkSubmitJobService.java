package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.model.SparkModel;
import org.springframework.stereotype.Service;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.SparkConf;

import java.util.List;

@Service
public class SparkSubmitJobService {
//    private final SparkConf sparkConf;
//    private final SparkModel sparkModel;
//
//    public SparkSubmitJobService(SparkConf sparkConf, SparkModel sparkModel) {
//        this.sparkConf = sparkConf;
//        this.sparkModel = sparkModel;
//    }
//
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
//
//    public void submitSparkJob() {
//        String[] sparkArgs = {
//                "--master", sparkConf.get("spark.master"),
//                "--name", sparkConf.get("spark.app.name"),
//                "--deploy-mode", sparkConf.get("spark.deploy.mode"),
//                "path/to/my-spark-job.jar"
//        };
//
//        SparkSubmit.main(sparkArgs);
//    }


}
