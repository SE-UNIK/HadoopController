package com.unik.hadoopcontroller.service;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Scanner;

@Service
public class HdfsReaderService {

    private static final Logger logger = LoggerFactory.getLogger(HdfsReaderService.class);

    @Autowired
    private FileSystem fileSystem;

    @Value("${spring.hadoop.fsUri}")
    private String fsDefaultFS;

    public void readFromHdfs() throws IOException {
        logger.info("Initializing Hadoop configuration");
        logger.info("Configured fs.defaultFS: {}", fsDefaultFS);

        Path filePath = new Path("/user/root/input/data.txt");
        logger.info("Reading from HDFS path: {}", filePath);

        try (FSDataInputStream inputStream = fileSystem.open(filePath); Scanner scanner = new Scanner(inputStream)) {
            logger.info("Successfully opened HDFS file for reading");
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                System.out.println(line);
            }
        } catch (IOException e) {
            logger.error("Error reading from HDFS", e);
            throw e;
        }
    }
}
