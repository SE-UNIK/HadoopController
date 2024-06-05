package com.unik.hadoopcontroller.repository;

import com.unik.hadoopcontroller.model.HdfsFileModel;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface HdfsFileRepository extends MongoRepository<HdfsFileModel, String> {
	

}
