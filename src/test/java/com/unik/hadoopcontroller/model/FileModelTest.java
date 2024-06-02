package com.unik.hadoopcontroller.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FileModelTest {

    @Test
    void testFileModel() {
        FileModel fileModel = new FileModel("test.txt", 1024, "file", "/path/to/test.txt");

        assertEquals("test.txt", fileModel.getFileName());
        assertEquals(1024, fileModel.getFileSize());
        assertEquals("file", fileModel.getFileType());
        assertEquals("/path/to/test.txt", fileModel.getFilePath());

        fileModel.setFileName("new.txt");
        fileModel.setFileSize(2048);
        fileModel.setFileType("document");
        fileModel.setFilePath("/new/path/to/test.txt");

        assertEquals("new.txt", fileModel.getFileName());
        assertEquals(2048, fileModel.getFileSize());
        assertEquals("document", fileModel.getFileType());
        assertEquals("/new/path/to/test.txt", fileModel.getFilePath());
    }
}
