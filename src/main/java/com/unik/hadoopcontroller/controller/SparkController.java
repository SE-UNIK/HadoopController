package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.model.SparkModel;
import com.unik.hadoopcontroller.service.SparkSubmitJobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.unik.hadoopcontroller.model.HdfsFileModel;

import java.io.IOException;
import java.util.List;

import com.unik.hadoopcontroller.service.HdfsDirectService;

@RestController
@RequestMapping("/spark")
@CrossOrigin(origins = "http://localhost:8081")
public class SparkController {

    @Autowired
    private SparkSubmitJobService sparkSubmitJobService;

    @Autowired
    private HdfsDirectService hdfsDirectService;

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


    @GetMapping("/results")
    public String getWordCountResults() throws IOException {
        String filePath = "/home/hadoop/wordcount/part-00000";
        return hdfsDirectService.readFromHdfs(filePath);
    }

    @GetMapping("/results/wordcount")
    public ResponseEntity<Resource> downloadWordcountResults() throws IOException {
        String filePath = "/home/hadoop/wordcount/part-00000";
        Resource resource = hdfsDirectService.downloadFileByPath(filePath);

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + resource.getFilename() + "\"")
                .body(resource);
    }
}
