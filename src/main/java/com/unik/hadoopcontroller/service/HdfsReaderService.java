package com.unik.hadoopcontroller.service;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

@Service
public class HdfsReaderService {

    private static final Logger logger = LoggerFactory.getLogger(HdfsReaderService.class);

    @Autowired
    private FileSystem fileSystem;

    @Value("${spring.hadoop.fsUri}")
    private String fsDefaultFS;

    public String readFromHdfs(String filePathStr) throws IOException {
        logger.info("Initializing Hadoop configuration");
        logger.info("Configured fs.defaultFS: {}", fsDefaultFS);

        Path filePath = new Path(filePathStr);
        logger.info("Reading from HDFS path: {}", filePath);

        StringBuilder content = new StringBuilder();
        try (FSDataInputStream inputStream = fileSystem.open(filePath); Scanner scanner = new Scanner(inputStream)) {
            logger.info("Successfully opened HDFS file for reading");
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                content.append(line).append(System.lineSeparator());
            }
        } catch (IOException e) {
            logger.error("Error reading from HDFS", e);
            throw e;
        }
        return content.toString();
    }

    public void writeToHdfs(String filePathStr, String content) throws IOException {
        Path filePath = new Path(filePathStr);
        if (!fileSystem.exists(filePath)) {
            fileSystem.create(filePath).write(content.getBytes());
            logger.info("Successfully written to HDFS file: {}", filePathStr);
        } else {
            logger.warn("File already exists: {}", filePathStr);
        }
    }

    public boolean deleteFile(String filePathStr) throws IOException {
        Path filePath = new Path(filePathStr);
        boolean result = fileSystem.delete(filePath, true);
        logger.info("File deletion {}: {}", filePathStr, result ? "successful" : "failed");
        return result;
    }

    public boolean fileExists(String filePathStr) throws IOException {
        Path filePath = new Path(filePathStr);
        boolean exists = fileSystem.exists(filePath);
        logger.info("File {} exists: {}", filePathStr, exists);
        return exists;
    }

    public List<String> listFiles(String directoryPathStr) throws IOException {
        Path directoryPath = new Path(directoryPathStr);
        FileStatus[] fileStatuses = fileSystem.listStatus(directoryPath);
        List<String> files = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            files.add(fileStatus.getPath().toString());
        }
        logger.info("Files in directory {}: {}", directoryPathStr, files);
        return files;
    }
}
