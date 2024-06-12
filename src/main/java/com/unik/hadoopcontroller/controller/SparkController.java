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

    @PostMapping("/submit")
    public String launchSparkJob(@RequestBody SparkModel sparkModel, @RequestParam String algorithmName) {
        System.out.println("Submitting Spark job " + sparkModel.toString());
        List<String> fileNames = sparkModel.getInputFileName();
        if (algorithmName.equals("wordcount")) {
            sparkSubmitJobService.launchWordcountSparkJob(fileNames);
        }
        else if(algorithmName.equals("kmeans")) {
            sparkSubmitJobService.launchKMeansSparkJob(fileNames);
        }
        else sparkSubmitJobService.launchLDASparkJob(fileNames);
        return "Spark job launched successfully";
    }

}
