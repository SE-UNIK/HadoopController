package com.unik.hadoopcontroller.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.unik.hadoopcontroller.model.MetadataModel;
import com.unik.hadoopcontroller.repository.HdfsFileRepository;
import com.unik.hadoopcontroller.repository.MetadataRepository;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;

@Service
public class DataTransferService {

    private static final Logger logger = LoggerFactory.getLogger(DataTransferService.class);

    @Autowired
    private FileSystem fileSystem;

    @Value("${spring.hadoop.fsUri}")
    private String fsDefaultFS;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private SparkSession sparkSession;

    public void transferMetadataToParquet(String directoryName, String id) {
        String directoryPathStr = "/user/hadoop/metadata/" + directoryName;
        Optional<MetadataModel> metadataModel = metadataService.getMetadataById(id);

        if (metadataModel.isPresent()) {
            try {
                Path directoryPath = new Path(directoryPathStr);

                // Load existing Parquet file if it exists
                Dataset<Row> existingData;
                if (fileSystem.exists(directoryPath)) {
                    existingData = sparkSession.read().parquet(directoryPathStr);
                } else {
                    existingData = sparkSession.createDataFrame(Collections.emptyList(), MetadataModel.class);
                }

                // Check if the ID already exists
                boolean idExists = !existingData.filter(functions.col("id").equalTo(id)).isEmpty();

                if (!idExists) {
                    // Convert MetadataModel to Dataset<Row>
                    MetadataModel model = metadataModel.get();
                    Dataset<Row> newData = sparkSession.createDataFrame(Collections.singletonList(model), MetadataModel.class);

                    // Append new data to existing data
                    Dataset<Row> updatedData = existingData.union(newData);

                    // Write updated data to Parquet
                    updatedData.write().mode("overwrite").parquet(directoryPathStr);

                    logger.info("Successfully transferred metadata to HDFS Parquet file: {}", directoryPathStr);
                } else {
                    logger.info("Metadata with ID {} already exists. Skipping write.", id);
                }
            } catch (IOException e) {
                logger.error("Error accessing HDFS", e);
            } catch (Exception e) {
                logger.error("Error transferring metadata to HDFS Parquet", e);
            }
        } else {
            logger.warn("Metadata with ID {} not found", id);
        }
    }

}
