package com.microservices.demo.reactive.elastic.query.service;

import com.microservices.demo.config.ElasticConfigData;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.elasticsearch.repository.config.EnableReactiveElasticsearchRepositories;

@SpringBootApplication
@ComponentScan(basePackages = {"com.microservices.demo"})
public class ReactiveElasticQueryServiceApplication {
    public static void main(String[] args) {

        SpringApplication.run(ReactiveElasticQueryServiceApplication.class, args);
    }
}
