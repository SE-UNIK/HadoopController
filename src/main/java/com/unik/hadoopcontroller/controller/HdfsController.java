package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.service.HdfsReaderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RequestMapping("/files")
@CrossOrigin(origins = "http://localhost:8081")
@RestController
public class HdfsController {

    @Autowired
    private HdfsReaderService hdfsReaderService;

    @GetMapping("/hadoop")
    public String readFromHdfs() {
        try {
            hdfsReaderService.readFromHdfs();
            return "Read from HDFS successfully.";
        } catch (IOException e) {
            return "Error reading from HDFS: " + e.getMessage();
        }
    }
}
