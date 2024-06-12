package com.unik.hadoopcontroller.model;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class SparkModel {

//    @Id
//    private String id;
    private String inputDirectoryPath;
    private List<String> inputFileName;
    private String outputDirectoryPath;
    private String outputFileName;
    private String algorithmName;                  // List<String>

//    public String getId() {
//        return id;
//    }
//
//    public void setId(String id) {
//        this.id = id;
//    }


    public List<String> getInputFileName() {
        return inputFileName;
    }

    public void setInputFileName(List<String> inputFileName) {
        this.inputFileName = inputFileName;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
    }

    public String getInputDirectoryPath() {
        return inputDirectoryPath;
    }

    public void setInputDirectoryPath(String inputDirectoryPath) {
        this.inputDirectoryPath = inputDirectoryPath;
    }

    public String getOutputDirectoryPath() {
        return outputDirectoryPath;
    }

    public void setOutputDirectoryPath(String outputDirectoryPath) {
        this.outputDirectoryPath = outputDirectoryPath;
    }

    public String getAlgorithmName() {
        return algorithmName;
    }

    public void setAlgorithmName(String algorithmName) {
        this.algorithmName = algorithmName;
    }


}
