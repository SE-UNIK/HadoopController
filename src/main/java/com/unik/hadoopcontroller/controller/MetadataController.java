package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.model.MetadataModel;
import com.unik.hadoopcontroller.service.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RequestMapping("/api")
@CrossOrigin(origins = "http://localhost:8081")
@RestController
public class MetadataController {

    @Autowired
    private MetadataService metadataService;

    @GetMapping("/metadata")
    public List<MetadataModel> getAllMetadata() {
        return metadataService.getAllMetadata();
    }

    @GetMapping("/metadata/{id}")
    public ResponseEntity<MetadataModel> getMetadataById(@PathVariable String id) {
        Optional<MetadataModel> metadata = metadataService.getMetadataById(id);
        return metadata.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/metadata")
    public MetadataModel createMetadata(@RequestBody MetadataModel metadataModel) {
        return metadataService.createMetadata(metadataModel);
    }

    @PutMapping("/metadata/{id}")
    public ResponseEntity<MetadataModel> updateMetadata(@PathVariable String id, @RequestBody MetadataModel updatedMetadataModel) {
        Optional<MetadataModel> metadata = metadataService.updateMetadata(id, updatedMetadataModel);
        return metadata.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/metadata/{id}")
    public ResponseEntity<Void> deleteMetadata(@PathVariable String id) {
        metadataService.deleteMetadata(id);
        return ResponseEntity.noContent().build();
    }
}
