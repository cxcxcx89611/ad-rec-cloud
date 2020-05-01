package com.imooc.ad;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

@EnableEurekaClient
@SpringBootApplication
public  class BinlogKafkaApplication {

        public static void main(String[] args) {
            SpringApplication.run(
                    BinlogKafkaApplication.class
            );
        }
    }
