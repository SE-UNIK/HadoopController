package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.model.FileModel;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest
class FileModelServiceTest {

    private FileModelService fileModelService;
    private FileSystem fileSystem;

    @BeforeEach
    void setUp() throws IOException {
        fileSystem = mock(FileSystem.class);
        fileModelService = new FileModelService() {
            @Override
            protected FileSystem getFileSystem() throws IOException {
                return fileSystem;
            }
        };
    }

    @Test
    void testGetAllFileModels() throws IOException {
        RemoteIterator<LocatedFileStatus> fileStatusIterator = mock(RemoteIterator.class);
        when(fileStatusIterator.hasNext()).thenReturn(true, false);
        LocatedFileStatus fileStatus = mock(LocatedFileStatus.class);
        when(fileStatusIterator.next()).thenReturn(fileStatus);
        when(fileSystem.listFiles(any(Path.class), eq(true))).thenReturn(fileStatusIterator);

        List<FileModel> fileModels = fileModelService.getAllFileModels();
        assertNotNull(fileModels);
        verify(fileSystem, times(1)).listFiles(any(Path.class), eq(true));
    }

    @Test
    void testGetFileModelById() throws IOException {
        Path path = new Path("/test.txt");
        when(fileSystem.exists(path)).thenReturn(true);
        when(fileSystem.getFileStatus(path)).thenReturn(mock(LocatedFileStatus.class));

        Optional<FileModel> fileModel = fileModelService.getFileModelById("test.txt");
        assertTrue(fileModel.isPresent());
        verify(fileSystem, times(1)).getFileStatus(path);
    }

    @Test
    void testCreateFileModel() throws IOException {
        FileModel fileModel = new FileModel("test.txt", 0, "file", "/test.txt");
        Path path = new Path(fileModel.getFilePath());
        when(fileSystem.exists(path)).thenReturn(false);

        FileModel createdFileModel = fileModelService.createFileModel(fileModel);
        assertEquals(fileModel, createdFileModel);
        verify(fileSystem, times(1)).create(path);
    }

    @Test
    void testUpdateFileModel() throws IOException {
        FileModel updatedFileModel = new FileModel("test.txt", 0, "file", "/new_test.txt");
        Path path = new Path("/test.txt");
        when(fileSystem.exists(path)).thenReturn(true);

        Optional<FileModel> result = fileModelService.updateFileModel("test.txt", updatedFileModel);
        assertTrue(result.isPresent());
        verify(fileSystem, times(1)).rename(any(Path.class), any(Path.class));
    }

    @Test
    void testDeleteFileModel() throws IOException {
        Path path = new Path("/test.txt");
        when(fileSystem.exists(path)).thenReturn(true);

        fileModelService.deleteFileModel("test.txt");
        verify(fileSystem, times(1)).delete(path, false);
    }
}
