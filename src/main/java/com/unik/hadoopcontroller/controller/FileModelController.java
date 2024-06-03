package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.model.FileModel;
import com.unik.hadoopcontroller.service.FileModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*") // Replace with your frontend URL
public class FileModelController {

    @Autowired
    private FileModelService fileModelService;

    @GetMapping("/files")
    public ResponseEntity<List<FileModel>> getAllFileModels() {
        try {
            List<FileModel> fileModels = fileModelService.getAllFileModels();
            return ResponseEntity.ok(fileModels);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }

    @GetMapping("/files/{fileName}")
    public ResponseEntity<FileModel> getFileModelById(@PathVariable String fileName) {
        try {
            Optional<FileModel> fileModel = fileModelService.getFileModelById(fileName);
            return fileModel.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }

    @PostMapping("/files")
    public ResponseEntity<FileModel> createFileModel(@RequestBody FileModel fileModel) {
        try {
            FileModel createdFileModel = fileModelService.createFileModel(fileModel);
            return ResponseEntity.ok(createdFileModel);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }

    @PutMapping("/files/{fileName}")
    public ResponseEntity<FileModel> updateFileModel(@PathVariable String fileName, @RequestBody FileModel updatedFileModel) {
        try {
            Optional<FileModel> fileModel = fileModelService.updateFileModel(fileName, updatedFileModel);
            return fileModel.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }

    @DeleteMapping("/files/{fileName}")
    public ResponseEntity<Void> deleteFileModel(@PathVariable String fileName) {
        try {
            fileModelService.deleteFileModel(fileName);
            return ResponseEntity.noContent().build();
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }
}
