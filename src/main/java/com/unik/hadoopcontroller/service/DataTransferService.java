package com.unik.hadoopcontroller.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unik.hadoopcontroller.model.MetadataModel;
import com.unik.hadoopcontroller.repository.HdfsFileRepository;
import com.unik.hadoopcontroller.repository.MetadataRepository;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@Service
public class DataTransferService {
	private static final Logger logger = LoggerFactory.getLogger(HdfsDirectService.class);

    @Autowired
    private FileSystem fileSystem;

    @Value("${spring.hadoop.fsUri}")
    private String fsDefaultFS;


	@Autowired
    private MetadataRepository metadataRepository;

	@Autowired
	private HdfsFileRepository hdfsFileRepository;

	MetadataService metadataService;
	HdfsDirectService hdfsDirectService;

	public void transferMetadataToHdfs(String fileName, String id) throws IOException {
		String filePathStr = "/user/hadoop/metadata/" + fileName;
		Path filePath = new Path(filePathStr);
		Optional<MetadataModel> metadataModel= metadataService.getMetadataById(id);
		if(metadataModel.isPresent()) {
			try {
				ObjectMapper objectMapper = new ObjectMapper();
				String jsonContent = objectMapper.writeValueAsString(metadataModel.get());
				hdfsDirectService.appendToHdfs(filePathStr, jsonContent);

			} catch (IOException e) {
				logger.error("Error transfering metadata to HDFS", e);
			}
		} else {
			logger.warn("Could not find metadata with id: ", id);
		}
	}






}
