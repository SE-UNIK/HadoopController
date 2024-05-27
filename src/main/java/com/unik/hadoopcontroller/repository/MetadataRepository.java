package com.unik.hadoopcontroller.repository;

import com.unik.hadoopcontroller.model.Metadata;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MetadataRepository extends MongoRepository<Metadata,String> {
}
