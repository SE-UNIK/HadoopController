package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.model.HdfsFileModel;
import com.unik.hadoopcontroller.model.SparkModel;
import com.unik.hadoopcontroller.service.HdfsFileModelService;
import com.unik.hadoopcontroller.service.SparkSubmitJobService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/spark")
@CrossOrigin(origins = "http://localhost:8081")
public class SparkController {

    @Autowired
    private SparkSubmitJobService sparkJobService;

    @PostMapping("/submit")
    public void submitSparkJob(@RequestBody SparkModel sparkJobModel) {
        sparkJobService.launchSparkJob(sparkJobModel);
    }

    // @RequestMapping("read-csv")
    // public ResponseEntity<String> getRowCount() {
    //     Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("../spark-spring-boot/src/main/resources/raw_data.csv");
    //     String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
    //             String.format("<h2>%s</h2>", "Spark version = "+sparkSession.sparkContext().version()) +
    //             String.format("<h3>%s</h3>", "Read csv..") +
    //             String.format("<h4>Total records %d</h4>", dataset.count()) +
    //             String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", dataset.schema().treeString()) +
    //             dataset.showString(20, 20, true);
    //     return ResponseEntity.ok(html);
    // }
}