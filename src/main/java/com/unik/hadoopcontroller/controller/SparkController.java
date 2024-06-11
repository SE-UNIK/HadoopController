package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.model.SparkModel;
import com.unik.hadoopcontroller.service.SparkSubmitJobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/spark")
public class SparkController {

    @Autowired
    private SparkSubmitJobService sparkSubmitJobService;

    @PostMapping("/submit")
    public String launchSparkJob(@RequestBody SparkModel sparkModel, @RequestParam String fileName, @RequestParam String hdfsFilePath) {
        System.out.println("Submitting Spark job " + sparkModel.toString());
        File outputFile = sparkSubmitJobService.launchSparkJob(sparkModel, fileName);
        if (outputFile != null) {
            return "Spark job submitted successfully. Output file: " + outputFile.getPath();
        } else {
            return "Failed to submit Spark job.";
        }
    }

}
