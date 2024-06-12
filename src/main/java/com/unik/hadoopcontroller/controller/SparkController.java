package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.model.SparkModel;
import com.unik.hadoopcontroller.service.SparkSubmitJobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/spark")
public class SparkController {

    @Autowired
    private SparkSubmitJobService sparkSubmitJobService;

    @PostMapping("/submit/wordcount")
    public String launchWordcountJob(@RequestBody SparkModel sparkModel) {
        System.out.println("Submitting Wordcount Spark job " + sparkModel.toString());
        List<String> fileNames = sparkModel.getInputFileName();
        sparkSubmitJobService.launchWordcountSparkJob(fileNames);
        return "Wordcount Spark job launched successfully";
    }

    @PostMapping("/submit/kmeans")
    public String launchKMeansJob(@RequestBody SparkModel sparkModel) {
        System.out.println("Submitting KMeans Spark job " + sparkModel.toString());
        List<String> fileNames = sparkModel.getInputFileName();
        sparkSubmitJobService.launchKMeansSparkJob(fileNames);
        return "KMeans Spark job launched successfully";
    }

    @PostMapping("/submit/tfidf")
    public String launchLDAJob(@RequestBody SparkModel sparkModel) {
        System.out.println("Submitting TFIDF Spark job " + sparkModel.toString());
        List<String> fileNames = sparkModel.getInputFileName();
        sparkSubmitJobService.launchTFIDFSparkJob(fileNames);
        return "LDA Spark job launched successfully";
    }
}
