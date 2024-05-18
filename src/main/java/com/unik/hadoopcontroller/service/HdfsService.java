package com.unik.hadoopcontroller.service;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class HdfsService {

	@Autowired
	private FileSystem fileSystem;

	public void writeFile(String path, String content) throws IOException {
		Path filePath = new Path(path);
		if(!fileSystem.exists(filePath)) {
			fileSystem.create(filePath).write(content.getBytes());
		}
	}
	public String readFile(String path) throws IOException {
		Path filePath = new Path(path);
		return new String(fileSystem.open(filePath).readAllBytes());
	}

}
