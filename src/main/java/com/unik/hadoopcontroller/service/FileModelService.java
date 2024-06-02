package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.model.FileModel;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class FileModelService {

    private final Map<String, FileModel> fileModelRepository = new HashMap<>();

    public List<FileModel> getAllFileModels() {
        return new ArrayList<>(fileModelRepository.values());
    }

    public Optional<FileModel> getFileModelById(String fileName) {
        return Optional.ofNullable(fileModelRepository.get(fileName));
    }

    public FileModel createFileModel(FileModel fileModel) {
        fileModelRepository.put(fileModel.getFileName(), fileModel);
        return fileModel;
    }

    public Optional<FileModel> updateFileModel(String fileName, FileModel updatedFileModel) {
        FileModel existingFileModel = fileModelRepository.get(fileName);
        if (existingFileModel != null) {
            existingFileModel.setFileSize(updatedFileModel.getFileSize());
            existingFileModel.setFileType(updatedFileModel.getFileType());
            existingFileModel.setFilePath(updatedFileModel.getFilePath());
            fileModelRepository.put(fileName, existingFileModel);
            return Optional.of(existingFileModel);
        }
        return Optional.empty();
    }

    public void deleteFileModel(String fileName) {
        fileModelRepository.remove(fileName);
    }
}