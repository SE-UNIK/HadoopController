package com.unik.hadoopcontroller;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class HadoopControllerApplication {

//	public static void main(String[] args) {
//		SpringApplication.run(HadoopControllerApplication.class, args);
//	}

	public static void main(String[] args) {
        System.out.println("MONGO_CLUSTER: " + System.getenv("MONGO_CLUSTER"));
        System.out.println("MONGO_DATABASE: " + System.getenv("MONGO_DATABASE"));
        SpringApplication.run(HadoopControllerApplication.class, args);
    }
}
