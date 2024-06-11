package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.model.MetadataModel;
import com.unik.hadoopcontroller.service.DataTransferService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api/data-transfer")
@CrossOrigin(origins = "*")
public class DataTransferController {

    @Autowired
    private DataTransferService dataTransferService;

    @PostMapping("/transfer")
    public ResponseEntity<String> transferMetadataToHDFS(@RequestBody List<String> ids) {
        try {
            dataTransferService.transferMetadataToHDFS(ids);
            return ResponseEntity.ok("Metadata successfully transferred to HDFS.");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error transferring metadata to HDFS: " + e.getMessage());
        }
    }

    @GetMapping("/hdfs")
    public List<MetadataModel> getMetadataFromHDFS() {
        try {
            return dataTransferService.getMetadataFromHDFS();
        } catch (IOException e) {
            throw new RuntimeException("Error reading metadata from HDFS", e);
        }
    }

    @GetMapping("/hdfs/download")
    public ResponseEntity<InputStreamResource> downloadMetadataFromHDFS() {
        try {
            return dataTransferService.downloadMetadataFromHDFS();
        } catch (IOException e) {
            throw new RuntimeException("Error downloading metadata from HDFS", e);
        }
    }
}
