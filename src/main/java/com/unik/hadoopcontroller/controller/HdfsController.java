package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.service.HdfsReaderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RequestMapping("/files")
@CrossOrigin(origins = "http://localhost:8081")
@RestController
public class HdfsController {

    @Autowired
    private HdfsReaderService hdfsReaderService;

    @GetMapping("/read")
    public String readFile(@RequestParam String path) {
        try {
            return hdfsReaderService.readFromHdfs(path);
        } catch (IOException e) {
            return "Error reading from HDFS: " + e.getMessage();
        }
    }

    @PostMapping("/write")
    public String writeFile(@RequestParam String path, @RequestParam String content) {
        try {
            hdfsReaderService.writeToHdfs(path, content);
            return "Successfully written to HDFS.";
        } catch (IOException e) {
            return "Error writing to HDFS: " + e.getMessage();
        }
    }

    @DeleteMapping("/delete")
    public String deleteFile(@RequestParam String path) {
        try {
            boolean deleted = hdfsReaderService.deleteFile(path);
            return deleted ? "File deleted successfully." : "File deletion failed.";
        } catch (IOException e) {
            return "Error deleting file from HDFS: " + e.getMessage();
        }
    }

    @GetMapping("/exists")
    public String fileExists(@RequestParam String path) {
        try {
            boolean exists = hdfsReaderService.fileExists(path);
            return exists ? "File exists." : "File does not exist.";
        } catch (IOException e) {
            return "Error checking file existence: " + e.getMessage();
        }
    }

    @GetMapping("/list")
    public List<String> listFiles(@RequestParam String directoryPath) {
        try {
            return hdfsReaderService.listFiles(directoryPath);
        } catch (IOException e) {
            return List.of("Error listing files in directory: " + e.getMessage());
        }
    }
}
