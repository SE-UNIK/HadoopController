package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.model.FileModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class FileModelService {

    protected static final String HDFS_URI = "hdfs://172.19.0.6:9000";
    protected static final Configuration configuration = new Configuration();

    static {
        configuration.set("fs.defaultFS", HDFS_URI);
    }

    protected FileSystem getFileSystem() throws IOException {
        return FileSystem.get(URI.create(HDFS_URI), configuration);
    }

    public List<FileModel> getAllFileModels() throws IOException {
        List<FileModel> fileModels = new ArrayList<>();
        FileSystem fs = getFileSystem();
        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path("/"), true);
        while (fileStatusListIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            fileModels.add(new FileModel(
                    fileStatus.getPath().getName(),
                    fileStatus.getLen(),
                    fileStatus.isDirectory() ? "directory" : "file",
                    fileStatus.getPath().toString()
            ));
        }
        fs.close();
        return fileModels;
    }

    public Optional<FileModel> getFileModelById(String fileName) throws IOException {
        FileSystem fs = getFileSystem();
        Path filePath = new Path("/" + fileName);
        if (fs.exists(filePath)) {
            FileStatus fileStatus = fs.getFileStatus(filePath);
            FileModel fileModel = new FileModel(
                    fileStatus.getPath().getName(),
                    fileStatus.getLen(),
                    fileStatus.isDirectory() ? "directory" : "file",
                    fileStatus.getPath().toString()
            );
            fs.close();
            return Optional.of(fileModel);
        } else {
            fs.close();
            return Optional.empty();
        }
    }

    public FileModel createFileModel(FileModel fileModel) throws IOException {
        FileSystem fs = getFileSystem();

        // Construct the full path in HDFS
        Path hdfsPath = new Path(fileModel.getFilePath());

        // Check if the file already exists
        if (fs.exists(hdfsPath)) {
            throw new IOException("File already exists: " + hdfsPath);
        }

        // Create a stream to read the file from the local filesystem
        InputStream inputStream = new FileInputStream(fileModel.getFilePath());

        // Create a stream to write to HDFS
        OutputStream outputStream = fs.create(hdfsPath);

        // Copy the contents from the local file to HDFS
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer))!= -1) {
            outputStream.write(buffer, 0, bytesRead);
        }

        // Close streams
        inputStream.close();
        outputStream.close();

        // Return the created FileModel (you might want to update it with the HDFS path)
        return fileModel;
    }

    public Optional<FileModel> updateFileModel(String fileName, FileModel updatedFileModel) throws IOException {
        FileSystem fs = getFileSystem();
        Path filePath = new Path("/" + fileName);
        if (fs.exists(filePath)) {
            // Assuming we can only update the file's metadata and not the content
            fs.rename(filePath, new Path(updatedFileModel.getFilePath()));
            fs.close();
            return Optional.of(updatedFileModel);
        } else {
            fs.close();
            return Optional.empty();
        }
    }

    public void deleteFileModel(String fileName) throws IOException {
        FileSystem fs = getFileSystem();
        Path filePath = new Path("/" + fileName);
        if (fs.exists(filePath)) {
            fs.delete(filePath, false);
        }
        fs.close();
    }

}
