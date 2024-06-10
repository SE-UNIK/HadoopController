package com.unik.hadoopcontroller.controller;

import com.unik.hadoopcontroller.model.HdfsFileModel;
import com.unik.hadoopcontroller.service.HdfsFileModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Optional;

@RequestMapping("/api")
@CrossOrigin(origins = "http://localhost:8081")
@RestController
public class HdfsFileModelController {

    @Autowired
    private HdfsFileModelService hdfsFileModelService;

    @GetMapping("/file")
    public List<HdfsFileModel> getAllHdfsFileModels() {
        return hdfsFileModelService.getAllHdfsFileModels();
    }

    @GetMapping("/file/{id}")
    public Optional<HdfsFileModel> getHdfsFileModelById(@PathVariable String id) {
        return hdfsFileModelService.getHdfsFileModelById(id);
    }

    @PostMapping("/file")
    public HdfsFileModel createHdfsFileModel(@RequestBody HdfsFileModel hdfsFileModel) {
        return hdfsFileModelService.createHdfsFileModel(hdfsFileModel);
    }

    @PutMapping("/file/{id}")
    public Optional<HdfsFileModel> updateHdfsFileModel(@PathVariable String id, @RequestBody HdfsFileModel updatedHdfsFileModel) {
        return hdfsFileModelService.updateHdfsFileModel(id, updatedHdfsFileModel);
    }

    @DeleteMapping("/file/{id}")
    public void deleteHdfsFile(@PathVariable String id) {
        hdfsFileModelService.deleteHdfsFile(id);
    }
}
