package com.unik.hadoopcontroller.model;

import java.io.Serializable;

public class FileModel implements Serializable {
    private static final long serialVersionUID = 1L;

    private String fileName;
    private long fileSize;
    private String fileType;
    private String filePath;

    public FileModel(String fileName, long fileSize, String fileType, String filePath) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.fileType = fileType;
        this.filePath = filePath;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
