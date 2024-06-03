package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.service.HdfsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RequestMapping("/api")
@CrossOrigin(origins = "http://localhost:8081")
@RestController
public class HdfsController {

    @Autowired
    private HdfsService hdfsService;

    @GetMapping("/read")
    public String readFile(@RequestParam String path) {
        try {
            return hdfsService.readFile(path);
        } catch (IOException e) {
            return "Error reading from HDFS: " + e.getMessage();
        }
    }

    @PostMapping("/write")
    public String writeFile(@RequestParam String path, @RequestParam String content) {
        try {
            hdfsService.writeFile(path, content);
            return "File written to HDFS successfully.";
        } catch (IOException e) {
            return "Error writing to HDFS: " + e.getMessage();
        }
    }

    @DeleteMapping("/delete")
    public String deleteFile(@RequestParam String path) {
        try {
            boolean deleted = hdfsService.deleteFile(path);
            return deleted ? "File deleted successfully." : "File deletion failed.";
        } catch (IOException e) {
            return "Error deleting file from HDFS: " + e.getMessage();
        }
    }

    @GetMapping("/exists")
    public boolean fileExists(@RequestParam String path) {
        try {
            return hdfsService.fileExists(path);
        } catch (IOException e) {
            return false;
        }
    }

    @GetMapping("/list")
    public List<String> listFiles(@RequestParam String directoryPath) {
        try {
            return hdfsService.listFiles(directoryPath);
        } catch (IOException e) {
            return null;
        }
    }
}
