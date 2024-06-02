package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.model.FileModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public abstract class FileModelService {

    private static final String HDFS_URI = "hdfs://your-hdfs-uri:8020";
    private static final Configuration configuration = new Configuration();

    static {
        configuration.set("fs.defaultFS", HDFS_URI);
    }

    private FileSystem getFileSystem() throws IOException {
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
        Path filePath = new Path(fileModel.getFilePath());
        if (!fs.exists(filePath)) {
            FSDataOutputStream outputStream = fs.create(filePath);
            outputStream.writeUTF("Sample content");  // Or you can write actual file content
            outputStream.close();
        }
        fs.close();
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
