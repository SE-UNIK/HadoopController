package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.model.MetadataModel;
import com.unik.hadoopcontroller.repository.MetadataRepository;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Optional;

@Service
public class DataTransferService {

    private static final Logger logger = LoggerFactory.getLogger(DataTransferService.class);

    @Autowired
    private FileSystem fileSystem;

    @Autowired
    private org.apache.hadoop.conf.Configuration hadoopConfiguration;

    @Value("${spring.hadoop.fsUri}")
    private String fsDefaultFS;


    @Autowired
    private MetadataService metadataService;

    @Autowired
    private HdfsFileModelService hdfsFileModelService;


    public void transferMetadataToParquet(String id) {
        String filePathStr = "/user/hadoop/metadata/metadataCollection.parquet";
        Path filePath = new Path(filePathStr);

        Optional<MetadataModel> metadataModel = metadataService.getMetadataById(id);

        if (metadataModel.isPresent()) {
            try {
                // Check if file exists
                if (fileSystem.exists(filePath)) {
                    // Read existing Parquet file to check if ID already exists
                    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(filePath, new Configuration())).build()) {
                        GenericRecord record;
                        while ((record = reader.read()) != null) {
                            if (record.get("id").toString().equals(id)) {
                                logger.warn("Metadata with ID {} already exists. Skipping write.", id);
                                return;
                            }
                        }
                    } catch (IOException e) {
                        logger.error("Error reading existing Parquet file", e);
                        return;
                    }
                }

                // Define Avro schema
                String schemaJson = "{"
                        + "\"type\":\"record\","
                        + "\"name\":\"MetadataModel\","
                        + "\"fields\":["
                        + "  {\"name\":\"id\", \"type\":\"string\"},"
                        + "  {\"name\":\"title\", \"type\":\"string\"},"
                        + "  {\"name\":\"publishDate\", \"type\":\"string\"},"
                        + "  {\"name\":\"authors\", \"type\":{\"type\":\"array\", \"items\": \"string\"}},"
                        + "  {\"name\":\"content\", \"type\":\"string\"}"
                        + "]}";
                Schema schema = new Schema.Parser().parse(schemaJson);

                // Convert MetadataModel to GenericRecord
                MetadataModel model = metadataModel.get();
                GenericRecord newRecord = new GenericData.Record(schema);
                newRecord.put("id", model.getId());
                newRecord.put("title", model.getTitle());
                newRecord.put("publishDate", model.getPublishDate().toString());
                newRecord.put("authors", model.getAuthors());
                newRecord.put("content", model.getContent());

                // Write to Parquet file
                try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(filePath)
                        .withSchema(schema)
                        .withConf(hadoopConfiguration)
                        .build()) {
                    writer.write(newRecord);
                }

                logger.info("Successfully transferred metadata to HDFS Parquet file: {}", filePathStr);
            } catch (IOException e) {
                logger.error("Error transferring metadata to HDFS Parquet", e);
            }
        } else {
            logger.warn("Metadata with ID {} not found", id);
        }
    }

    public void processJson(String collectionName) {
        // Implementation for processing JSON
    }
}
