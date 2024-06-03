package com.unik.hadoopcontroller.service;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class HdfsService {

    @Autowired
    private FileSystem fileSystem;

    public void writeFile(String path, String content) throws IOException {
        Path filePath = new Path(path);
        if (!fileSystem.exists(filePath)) {
            fileSystem.create(filePath).write(content.getBytes());
        }
    }

    public String readFile(String path) throws IOException {
        Path filePath = new Path(path);
        return new String(fileSystem.open(filePath).readAllBytes());
    }

    public boolean deleteFile(String path) throws IOException {
        Path filePath = new Path(path);
        return fileSystem.delete(filePath, true); // 'true' for recursive delete
    }

    public boolean fileExists(String path) throws IOException {
        Path filePath = new Path(path);
        return fileSystem.exists(filePath);
    }

    public List<String> listFiles(String directoryPath) throws IOException {
        Path path = new Path(directoryPath);
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        List<String> files = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            files.add(fileStatus.getPath().toString());
        }
        return files;
    }
}
