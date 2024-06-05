package com.unik.hadoopcontroller.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@Configuration
public class SparkConfig {

    @Value("${spark.master}")
    private String sparkMaster;

    @Value("${spark.app.name}")
    private String sparkAppName;

    @Bean
    public JavaSparkContext javaSparkContext() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(sparkAppName)
                .setMaster(sparkMaster);
        return new JavaSparkContext(sparkConf);
    }

    @Bean
    public SparkSession sparkSession(JavaSparkContext javaSparkContext) {
        SparkSession sparkSession = SparkSession.builder()
                .sparkContext(javaSparkContext.sc())
                .getOrCreate();

        // Register a shutdown hook to close SparkContext when the application exits
        Runtime.getRuntime().addShutdownHook(new Thread(javaSparkContext::stop));

        return sparkSession;
    }
}
