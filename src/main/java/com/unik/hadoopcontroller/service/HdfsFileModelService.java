package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.model.HdfsFileModel;
import com.unik.hadoopcontroller.repository.HdfsFileRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class HdfsFileModelService {

    @Autowired
    private HdfsFileRepository hdfsFileRepository;

    public List<HdfsFileModel> getAllHdfsFileModels() {
        return hdfsFileRepository.findAll();
    }

    public Optional<HdfsFileModel> getHdfsFileModelById(String id) {
        return hdfsFileRepository.findById(id);
    }

    public HdfsFileModel createHdfsFileModel(HdfsFileModel hdfsFileModel) {
        return hdfsFileRepository.save(hdfsFileModel);
    }

    public Optional<HdfsFileModel> updateHdfsFileModel(String id, HdfsFileModel updatedHdfsFileModel) {
        return hdfsFileRepository.findById(id).map(existingHdfsFileModel -> {
            existingHdfsFileModel.setFileName(updatedHdfsFileModel.getFileName());
            existingHdfsFileModel.setTitle(updatedHdfsFileModel.getTitle());
            existingHdfsFileModel.setFilePath(updatedHdfsFileModel.getFilePath());
            existingHdfsFileModel.setAuthors(updatedHdfsFileModel.getAuthors());
            existingHdfsFileModel.setFileSize(updatedHdfsFileModel.getFileSize());
            return hdfsFileRepository.save(existingHdfsFileModel);
        });
    }

    public void deleteHdfsFile(String id) {
        hdfsFileRepository.deleteById(id);
    }
}
