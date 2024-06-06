package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.service.DataTransferService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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

    @GetMapping("/parquet")
    public List<Map<String, Object>> readParquetFile() throws IOException {
        return dataTransferService.readParquetFile();
    }
}
