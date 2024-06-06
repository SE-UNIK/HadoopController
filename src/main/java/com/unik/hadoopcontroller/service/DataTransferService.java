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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class DataTransferService {

    private static final Logger logger = LoggerFactory.getLogger(DataTransferService.class);

    @Autowired
    private FileSystem fileSystem;

    @Autowired
    private org.apache.hadoop.conf.Configuration hadoopConfiguration;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private HdfsFileModelService hdfsFileModelService;

    public void transferMetadataToParquet(List<String> ids) {
        String filePathStr = "/user/hadoop/metadata/metadataCollection.parquet";
        Path filePath = new Path(filePathStr);

        List<GenericRecord> newRecords = new ArrayList<>();
        Schema schema = getAvroSchema();

        // Collect new records
        for (String id : ids) {
            Optional<MetadataModel> metadataModel = metadataService.getMetadataById(id);
            if (metadataModel.isPresent()) {
                MetadataModel model = metadataModel.get();
                GenericRecord newRecord = new GenericData.Record(schema);
                newRecord.put("id", model.getId());
                newRecord.put("title", model.getTitle());
                newRecord.put("publishDate", model.getPublishDate().toString());
                newRecord.put("authors", model.getAuthors());
                newRecord.put("content", model.getContent());
                newRecords.add(newRecord);
            } else {
                logger.warn("Metadata with ID {} not found", id);
            }
        }

        try {
            List<GenericRecord> records = new ArrayList<>();

            // Read existing records if the file exists
            if (fileSystem.exists(filePath)) {
                try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(filePath, hadoopConfiguration)).build()) {
                    GenericRecord record;
                    while ((record = reader.read()) != null) {
                        records.add(record);
                    }
                } catch (IOException e) {
                    logger.error("Error reading existing Parquet file", e);
                    return;
                }
            }

            // Add new records to the existing records
            records.addAll(newRecords);

            // Write all records to Parquet file
            Path tempFilePath = new Path(filePathStr + ".tmp");
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(tempFilePath)
                    .withSchema(schema)
                    .withConf(hadoopConfiguration)
                    .build()) {
                for (GenericRecord rec : records) {
                    writer.write(rec);
                }
            }

            // Replace the old file with the new one
            fileSystem.delete(filePath, false);
            fileSystem.rename(tempFilePath, filePath);

            logger.info("Successfully transferred metadata to HDFS Parquet file: {}", filePathStr);
        } catch (IOException e) {
            logger.error("Error transferring metadata to HDFS Parquet", e);
        }
    }

    private Schema getAvroSchema() {
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
        return new Schema.Parser().parse(schemaJson);
    }

    public void processJson(String collectionName) {
        // Implementation for processing JSON
    }
}
