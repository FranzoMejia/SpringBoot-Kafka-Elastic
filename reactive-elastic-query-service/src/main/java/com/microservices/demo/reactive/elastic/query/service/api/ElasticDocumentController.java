package com.microservices.demo.reactive.elastic.query.service.api;

import com.microservices.demo.config.ElasticConfigData;
import com.microservices.demo.elastic.query.service.common.model.ElasticQueryServiceRequestModel;
import com.microservices.demo.elastic.query.service.common.model.ElasticQueryServiceResponseModel;
import com.microservices.demo.reactive.elastic.query.service.business.ElasticQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import jakarta.validation.Valid;

@Controller
@RequestMapping(value = "/documents")
public class ElasticDocumentController {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticDocumentController.class);

    private final ElasticQueryService elasticQueryService;
    private final ElasticConfigData elasticConfigData;

    public ElasticDocumentController(ElasticQueryService queryService, ElasticConfigData elasticConfigData) {
        this.elasticQueryService = queryService;
        this.elasticConfigData = elasticConfigData;
    }

    @PostMapping(value = "/get-doc-by-text",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE,
            consumes = MediaType.APPLICATION_JSON_VALUE)
    public Flux<ElasticQueryServiceResponseModel> getDocumentByText(
            @RequestBody @Valid ElasticQueryServiceRequestModel requestModel) {
        return elasticQueryService.getDocumentByText(requestModel.getText())
                .doOnSubscribe(subscription -> LOG.info("Querying index: {}", elasticConfigData.getIndexName()))
                .doOnNext(response -> LOG.info("Document found: {}", response))
                .doOnError(error -> LOG.error("Error occurred while querying documents: {}", error.getMessage()))
                .log();
    }
}
