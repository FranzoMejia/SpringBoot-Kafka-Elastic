package com.microservices.demo.reactive.elastic.query.service.api;

import com.microservices.demo.config.ElasticConfigData;
import com.microservices.demo.elastic.query.service.common.model.ElasticQueryServiceRequestModel;
import com.microservices.demo.elastic.query.service.common.model.ElasticQueryServiceResponseModel;
import com.microservices.demo.reactive.elastic.query.service.business.ElasticQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import jakarta.validation.Valid;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

@RestController
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
            produces = MediaType.APPLICATION_JSON_VALUE,
            consumes = MediaType.APPLICATION_JSON_VALUE)

    public Flux<ElasticQueryServiceResponseModel> getDocumentByText(
            @RequestBody @Valid ElasticQueryServiceRequestModel requestModel) {
        Flux<ElasticQueryServiceResponseModel> response =
               elasticQueryService.getDocumentByText(requestModel.getText());
        response=response.log();
        LOG.info("Returning from query reactive service for text {}!", requestModel.getText());
        return response;
    }

    @GetMapping(value = "/get-doc-by-text",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE,
            consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<ElasticQueryServiceResponseModel> getDocumentByText2(
            @RequestBody @Valid ElasticQueryServiceRequestModel requestModel) {
        // Flux<ElasticQueryServiceResponseModel> response =
        //        elasticQueryService.getDocumentByText(requestModel.getText());
        // response=response.log();
        List<ElasticQueryServiceResponseModel> response = List.of(
                        new ElasticQueryServiceResponseModel("1", 1L ,"text1", LocalDateTime.now()),
                        new ElasticQueryServiceResponseModel("2",2L ,"text2", LocalDateTime.now()),
                        new ElasticQueryServiceResponseModel("3",3L , "text3", LocalDateTime.now()));

        LOG.info("Returning from query reactive service for text {}!", requestModel.getText());
        return response;
    }
}
