package com.unik.hadoopcontroller.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spring.hadoop.fsUri}")
    private String fsUri;

    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.master}")
    private String master;

    @Value("${spark.deploymode}")
    private String deployMode;

    @Bean
    public SparkConf sparkConf() {
//        SparkConf sparkConf = new SparkConf()
//                .setAppName(appName)
//                .setMaster(master);
//        // Set Hadoop configuration in SparkConf
//        sparkConf.set("fs.defaultFS", fsUri);
//        sparkConf.set("spark.submit.deployMode", deployMode);
//        sparkConf.set("spark.logConf", "true");

        System.out.println("FS URI: " + fsUri);
        System.out.println("App Name: " + appName);
        System.out.println("Master: " + master);
        System.out.println("Deploy Mode: " + deployMode);

        try {
            SparkConf sparkConf = new SparkConf()
                    .setAppName(appName)
                    .setMaster(master)
                    .set("fs.defaultFS", fsUri)
                    .set("spark.submit.deployMode", deployMode)
                    .set("spark.logConf", "true");

            System.out.println("SparkConf initialized successfully.");
            return sparkConf;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error initializing SparkConf", e);
        }

//        return sparkConf;
    }
//    @Bean
//    public JavaSparkContext javaSparkContext() {
//        return new JavaSparkContext(sparkConf());
//    }
}
