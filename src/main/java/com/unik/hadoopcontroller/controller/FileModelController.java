package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.model.FileModel;
import com.unik.hadoopcontroller.service.FileModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "http://localhost:3000") // Replace with your frontend URL
public class FileModelController {

    @Autowired
    private FileModelService fileModelService;

    @GetMapping("/files")
    public List<FileModel> getAllFileModels() {
        return fileModelService.getAllFileModels();
    }

    @GetMapping("/files/{fileName}")
    public ResponseEntity<FileModel> getFileModelById(@PathVariable String fileName) {
        Optional<FileModel> fileModel = fileModelService.getFileModelById(fileName);
        return fileModel.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/files")
    public FileModel createFileModel(@RequestBody FileModel fileModel) {
        return fileModelService.createFileModel(fileModel);
    }

    @PutMapping("/files/{fileName}")
    public ResponseEntity<FileModel> updateFileModel(@PathVariable String fileName, @RequestBody FileModel updatedFileModel) {
        Optional<FileModel> fileModel = fileModelService.updateFileModel(fileName, updatedFileModel);
        return fileModel.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/files/{fileName}")
    public ResponseEntity<Void> deleteFileModel(@PathVariable String fileName) {
        fileModelService.deleteFileModel(fileName);
        return ResponseEntity.noContent().build();
    }
}