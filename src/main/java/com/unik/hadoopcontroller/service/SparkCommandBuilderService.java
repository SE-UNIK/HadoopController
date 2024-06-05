package com.unik.hadoopcontroller.service;

import org.springframework.stereotype.Component;

@Component
public class SparkCommandBuilderService {

    public String buildSparkCommand(String inputPath, String outputPath, String algorithm) {
        // Construct Spark command based on user inputs
        return "spark-submit --master yarn --deploy-mode cluster " + algorithm + " input.jar " + inputPath + " " + outputPath;
        //return sparkCommand;
    }
}

