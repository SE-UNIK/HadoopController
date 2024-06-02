package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.model.FileModel;
import com.unik.hadoopcontroller.service.FileModelService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import java.util.Arrays;
import java.util.Optional;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(FileModelController.class)
class FileModelControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private FileModelService fileModelService;

    @Test
    void testGetAllFileModels() throws Exception {
        when(fileModelService.getAllFileModels()).thenReturn(Arrays.asList(new FileModel("test.txt", 1024, "file", "/test.txt")));

        mockMvc.perform(get("/api/files"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].fileName").value("test.txt"));
    }

    @Test
    void testGetFileModelById() throws Exception {
        when(fileModelService.getFileModelById("test.txt")).thenReturn(Optional.of(new FileModel("test.txt", 1024, "file", "/test.txt")));

        mockMvc.perform(get("/api/files/test.txt"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.fileName").value("test.txt"));
    }

    @Test
    void testCreateFileModel() throws Exception {
        FileModel fileModel = new FileModel("test.txt", 1024, "file", "/test.txt");
        when(fileModelService.createFileModel(any(FileModel.class))).thenReturn(fileModel);

        mockMvc.perform(post("/api/files")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"fileName\":\"test.txt\",\"fileSize\":1024,\"fileType\":\"file\",\"filePath\":\"/test.txt\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.fileName").value("test.txt"));
    }

    @Test
    void testUpdateFileModel() throws Exception {
        FileModel fileModel = new FileModel("test.txt", 1024, "file", "/new_test.txt");
        when(fileModelService.updateFileModel(any(String.class), any(FileModel.class))).thenReturn(Optional.of(fileModel));

        mockMvc.perform(put("/api/files/test.txt")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"fileName\":\"test.txt\",\"fileSize\":1024,\"fileType\":\"file\",\"filePath\":\"/new_test.txt\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.filePath").value("/new_test.txt"));
    }

    @Test
    void testDeleteFileModel() throws Exception {
        Mockito.doNothing().when(fileModelService).deleteFileModel(any(String.class));

        mockMvc.perform(delete("/api/files/test.txt"))
                .andExpect(status().isNoContent());
    }
}