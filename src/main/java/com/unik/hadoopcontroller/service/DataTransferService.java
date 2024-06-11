package com.unik.hadoopcontroller.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unik.hadoopcontroller.model.MetadataModel;
import com.unik.hadoopcontroller.repository.MetadataRepository;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class DataTransferService {

    @Autowired
    private FileSystem fileSystem;

    @Autowired
    private MetadataRepository metadataRepository;

    public void transferMetadataToHDFS(List<String> ids) throws IOException {
        String filePathStr = "/user/hadoop/metadata/metadataCollection.json";
        Path filePath = new Path(filePathStr);

        List<MetadataModel> metadataList = new ArrayList<>();

        for (String id : ids) {
            Optional<MetadataModel> metadata = metadataRepository.findById(id);
            metadata.ifPresent(metadataList::add);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(metadataList);

        try (FSDataOutputStream outputStream = fileSystem.create(filePath, true)) {
            outputStream.writeUTF(json);
        }
    }

    public List<MetadataModel> getMetadataFromHDFS() throws IOException {
        String filePathStr = "/user/hadoop/metadata/metadataCollection.json";
        Path filePath = new Path(filePathStr);

        try (FSDataInputStream inputStream = fileSystem.open(filePath);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(inputStream, outputStream, fileSystem.getConf(), false);
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(outputStream.toString(), new TypeReference<List<MetadataModel>>() {});
        }
    }

    public ResponseEntity<InputStreamResource> downloadMetadataFromHDFS() throws IOException {
        String filePathStr = "/user/hadoop/metadata/metadataCollection.json";
        Path filePath = new Path(filePathStr);

        FSDataInputStream inputStream = fileSystem.open(filePath);
        InputStreamResource resource = new InputStreamResource(inputStream);

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=metadataCollection.json")
                .contentType(MediaType.APPLICATION_JSON)
                .body(resource);
    }
}
