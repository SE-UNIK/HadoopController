package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.model.HdfsFileModel;
import com.unik.hadoopcontroller.repository.HdfsFileRepository;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

@Service
public class HdfsDirectService {

    private static final Logger logger = LoggerFactory.getLogger(HdfsDirectService.class);

    @Autowired
    private FileSystem fileSystem;

    @Value("${spring.hadoop.fsUri}")
    private String fsDefaultFS;

    @Autowired
    private HdfsFileRepository hdfsFileRepository;

    public String readFromHdfs(String filePathStr) throws IOException {
        logger.info("Initializing Hadoop configuration");
        logger.info("Configured fs.defaultFS: {}", fsDefaultFS);

        Path filePath = new Path(filePathStr);
        logger.info("Reading from HDFS path: {}", filePath);

        StringBuilder content = new StringBuilder();
        try (FSDataInputStream inputStream = fileSystem.open(filePath);
             Scanner scanner = new Scanner(inputStream)) {
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

    public void appendToHdfs(String filePathStr, String content) throws IOException {
        Path filePath = new Path(filePathStr);
        if (fileSystem.exists(filePath)) {
            try (FSDataOutputStream outputStream = fileSystem.append(filePath)) {
                outputStream.write(content.getBytes());
                logger.info("Successfully appended to HDFS file: {}", filePathStr);
            } catch (IOException e) {
                logger.error("Error appending to HDFS", e);
                throw e;
            }
        } else {
            logger.warn("File does not exist: {}", filePathStr);
            throw new IOException("File does not exist: " + filePathStr);
        }
    }
    public void writeToHdfsUnique(String originalFileName, MultipartFile file,String title, List<String> authors) throws IOException {
        String uniqueFilePathStr = "/user/hadoop/inputs/" + UUID.randomUUID().toString() + "_" + originalFileName;
        Path filePath = new Path(uniqueFilePathStr);
        try (FSDataOutputStream outputStream = fileSystem.create(filePath)) {
            outputStream.write(file.getBytes());
            // Save the file metadata to MongoDB
            HdfsFileModel hdfsFileModel = new HdfsFileModel();
            hdfsFileModel.setFileName(originalFileName);
            hdfsFileModel.setFilePath(uniqueFilePathStr);
            hdfsFileModel.setFileSize(file.getSize());
            hdfsFileModel.setTitle(title);
            hdfsFileModel.setAuthors(authors);
            // Add other metadata fields as needed
            hdfsFileRepository.save(hdfsFileModel);
            logger.info("Successfully written to HDFS file: {}", uniqueFilePathStr);
        } catch (IOException e) {
            logger.error("Error writing to HDFS", e);
            throw e;
        }
    }
    public void writeToHdfsUniqueWithFilePath(String filePath,String originalFileName, MultipartFile file,String title, List<String> authors) throws IOException {
        String uniqueFilePathStr = filePath + UUID.randomUUID().toString() + "_" + originalFileName;
        Path filePaths = new Path(uniqueFilePathStr);
        try (FSDataOutputStream outputStream = fileSystem.create(filePaths)) {
            outputStream.write(file.getBytes());
            // Save the file metadata to MongoDB
            HdfsFileModel hdfsFileModel = new HdfsFileModel();
            hdfsFileModel.setFileName(originalFileName);
            hdfsFileModel.setFilePath(uniqueFilePathStr);
            hdfsFileModel.setFileSize(file.getSize());
            hdfsFileModel.setTitle(title);
            hdfsFileModel.setAuthors(authors);
            // Add other metadata fields as needed
            hdfsFileRepository.save(hdfsFileModel);
            logger.info("Successfully written to HDFS file: {}", uniqueFilePathStr);
        } catch (IOException e) {
            logger.error("Error writing to HDFS", e);
            throw e;
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
    public Resource downloadFile(String fileName) throws IOException {
        String filePathStr = "/user/hadoop/outputs/" + fileName;
        Path filePath = new Path(filePathStr);

        if (!fileSystem.exists(filePath)) {
            throw new FileNotFoundException("File not found in HDFS: " + filePathStr);
        }

        FSDataInputStream inputStream = fileSystem.open(filePath);
        return new InputStreamResource(inputStream);
    }
     public File getFileFromHdfs(String filePathStr) throws IOException {
        Path filePath = new Path(filePathStr);
        File tempFile = File.createTempFile("hdfs-", "-download");
        tempFile.deleteOnExit();

        try (FSDataInputStream inputStream = fileSystem.open(filePath);
             FileOutputStream outputStream = new FileOutputStream(tempFile)) {
            IOUtils.copy(inputStream, outputStream);
        }

        return tempFile;
    }

}
