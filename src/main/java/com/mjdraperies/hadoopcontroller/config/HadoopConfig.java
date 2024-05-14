package com.mjdraperies.hadoopcontroller.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

import java.io.IOException;

@org.springframework.context.annotation.Configuration
public class HadoopConfig {
	@Bean
	public Configuration hadoopConfig() {
		Configuration config = new  Configuration();
        config.set("fs.defaultFS", "hdfs://namenode:9000");
        config.set("dfs.replication", "1");
        return config;
	}

	@Bean
	public FileSystem fileSystem(Configuration config) throws IOException {
		return FileSystem.get(config);
	}
}
