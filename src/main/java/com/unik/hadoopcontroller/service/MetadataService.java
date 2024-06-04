package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.model.MetadataModel;
import com.unik.hadoopcontroller.repository.MetadataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class MetadataService {

    @Autowired
    private MetadataRepository metadataRepository;

    public List<MetadataModel> getAllMetadata() {
        return metadataRepository.findAll();
    }

    public Optional<MetadataModel> getMetadataById(String id) {
        return metadataRepository.findById(id);
    }

    public MetadataModel createMetadata(MetadataModel metadataModel) {
        return metadataRepository.save(metadataModel);
    }

    public Optional<MetadataModel> updateMetadata(String id, MetadataModel updatedMetadataModel) {
        return metadataRepository.findById(id).map(existingMetadataModel -> {
            existingMetadataModel.setTitle(updatedMetadataModel.getTitle());
            existingMetadataModel.setPublishDate(updatedMetadataModel.getPublishDate());
            existingMetadataModel.setAuthors(updatedMetadataModel.getAuthors());
            existingMetadataModel.setContent(updatedMetadataModel.getContent());
            return metadataRepository.save(existingMetadataModel);
        });
    }

    public void deleteMetadata(String id) {
        metadataRepository.deleteById(id);
    }
}
