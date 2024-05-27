package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.model.Metadata;
import com.unik.hadoopcontroller.service.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RequestMapping("/api")
@CrossOrigin(origins = "http://localhost:3000") // Replace with your frontend URL
@RestController
public class MetadataController {

    @Autowired
    private MetadataService metadataService;

    @GetMapping("/metadata")
    public List<Metadata> getAllMetadata() {
        return metadataService.getAllMetadata();
    }

    @GetMapping("/metadata/{id}")
    public ResponseEntity<Metadata> getMetadataById(@PathVariable String id) {
        Optional<Metadata> metadata = metadataService.getMetadataById(id);
        return metadata.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/metadata")
    public Metadata createMetadata(@RequestBody Metadata metadata) {
        return metadataService.createMetadata(metadata);
    }

    @PutMapping("/metadata/{id}")
    public ResponseEntity<Metadata> updateMetadata(@PathVariable String id, @RequestBody Metadata updatedMetadata) {
        Optional<Metadata> metadata = metadataService.updateMetadata(id, updatedMetadata);
        return metadata.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/metadata/{id}")
    public ResponseEntity<Void> deleteMetadata(@PathVariable String id) {
        metadataService.deleteMetadata(id);
        return ResponseEntity.noContent().build();
    }
}
