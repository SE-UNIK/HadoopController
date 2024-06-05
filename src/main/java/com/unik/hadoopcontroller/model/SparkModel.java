package com.unik.hadoopcontroller.model;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.data.annotation.Id;

import java.util.List;

public class SparkModel {

    @Id
    private String id;
    private String inputPath;
    private String outputPath;
    private List<String> algorithm;
    private Dataset<Row> results;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public List<String> getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(List<String> algorithm) {
        this.algorithm = algorithm;
    }

    public Dataset<Row> getResults() {
        return results;
    }

    public void setResults(Dataset<Row> results) {
        this.results = results;
    }


}
