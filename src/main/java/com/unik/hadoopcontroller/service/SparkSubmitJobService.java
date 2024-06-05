package com.unik.hadoopcontroller.service;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

@Service
public class SparkSubmitJobService {

    @Autowired
    private SparkCommandBuilder commandBuilder;

    public void executeSparkJob(String inputPath, String outputPath, String algorithm) {
        // Convert user inputs into Spark command
        String sparkCommand = commandBuilder.buildSparkCommand(inputPath, outputPath, algorithm);

        // Execute Spark job (e.g., submit as shell command or run programmatically)
        executeSparkCommand(sparkCommand);
    }

    private void executeSparkCommand(String sparkCommand) {
        // Execute Spark job (e.g., using Runtime.exec() for shell commands)
        // Handle error handling and result processing here
    }
}