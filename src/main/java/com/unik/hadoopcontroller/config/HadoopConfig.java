package com.unik.hadoopcontroller.config;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;

@Configuration
public class HadoopConfig {

    @Value("${spring.hadoop.fsUri}")
    private String fsUri;

    @Value("${spring.hadoop.config.fs.defaultFS}")
    private String defaultFS;

    @Value("${spring.hadoop.config.dfs.client.use.datanode.hostname}")
    private boolean useDatanodeHostname;

    @Value("${spring.hadoop.config.dfs.replication}")
    private int replication;

    @Bean
    public org.apache.hadoop.conf.Configuration hadoopConfiguration() {
        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
        config.set("fs.defaultFS", defaultFS);
        config.setBoolean("dfs.client.use.datanode.hostname", useDatanodeHostname);
        config.setInt("dfs.replication", replication);
        return config;
    }

    @Bean
    public FileSystem fileSystem(org.apache.hadoop.conf.Configuration hadoopConfiguration) throws IOException {
        return FileSystem.get(URI.create(fsUri), hadoopConfiguration);
    }

}
