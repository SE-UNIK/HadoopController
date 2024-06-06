package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.service.DataTransferService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RequestMapping("/data")
@CrossOrigin(origins = "http://localhost:8081")
@RestController
public class DataTransferController {

    @Autowired
    private DataTransferService dataTransferService;

    @PostMapping("/transfer")
    public String transferMetadataToParquet(@RequestBody List<String> ids) {
        try {
            dataTransferService.transferMetadataToParquet(ids);
            return "Metadata successfully transferred to Parquet file.";
        } catch (Exception e) {
            return "Error transferring metadata to Parquet file: " + e.getMessage();
        }
    }
}