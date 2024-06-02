package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.model.Metadata;
import com.unik.hadoopcontroller.repository.MetadataRepository;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.OutputStream;
import java.util.List;

@Service
public class DataTransferService {
    @Autowired
    private MetadataRepository metadataRepository;

    @Autowired
    private FileSystem hadoopFileSystem;

    public void transferMetadataToHDFS() {
        List<Metadata> metadataList = metadataRepository.findAll();
        for (Metadata metadata : metadataList) {
            try (OutputStream out = hadoopFileSystem.create(new Path("/metadata" + metadata.getId() + ".json"))) {
                out.write(metadata.toString().getBytes());
                out.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }



}
