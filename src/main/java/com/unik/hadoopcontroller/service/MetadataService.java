package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.model.Metadata;
import com.unik.hadoopcontroller.repository.MetadataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class MetadataService {

    @Autowired
    private MetadataRepository metadataRepository;

    public List<Metadata> getAllMetadata() {
        return metadataRepository.findAll();
    }

    public Optional<Metadata> getMetadataById(String id) {
        return metadataRepository.findById(id);
    }

    public Metadata createMetadata(Metadata metadata) {
        return metadataRepository.save(metadata);
    }

    public Optional<Metadata> updateMetadata(String id, Metadata updatedMetadata) {
        return metadataRepository.findById(id).map(existingMetadata -> {
            existingMetadata.setTitle(updatedMetadata.getTitle());
            existingMetadata.setPublishDate(updatedMetadata.getPublishDate());
            existingMetadata.setAuthors(updatedMetadata.getAuthors());
            existingMetadata.setContent(updatedMetadata.getContent());
            return metadataRepository.save(existingMetadata);
        });
    }

    public void deleteMetadata(String id) {
        metadataRepository.deleteById(id);
    }
}
