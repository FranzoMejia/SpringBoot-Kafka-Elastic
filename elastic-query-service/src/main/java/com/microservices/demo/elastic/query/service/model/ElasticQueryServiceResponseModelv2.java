package com.microservices.demo.elastic.query.service.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.hateoas.RepresentationModel;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ElasticQueryServiceResponseModelv2 extends RepresentationModel<ElasticQueryServiceResponseModelv2> {
    private Long id;
    private Long userId;
    private String text;
    private LocalDateTime createdAt;
}
