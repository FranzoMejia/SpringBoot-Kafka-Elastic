package com.microservices.demo.elastic.query.service.api;

import com.microservices.demo.elastic.query.service.business.ElasticQueryService;
import com.microservices.demo.elastic.query.service.model.ElasticQueryServiceRequestModel;
import com.microservices.demo.elastic.query.service.model.ElasticQueryServiceResponseModel;
import com.microservices.demo.elastic.query.service.model.ElasticQueryServiceResponseModelv2;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/documents")
public class ElasticDocumentController {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticDocumentController.class);
    private final ElasticQueryService elasticQueryService;

    public ElasticDocumentController(ElasticQueryService elasticQueryService) {
        this.elasticQueryService = elasticQueryService;
    }


    @GetMapping("/v1")
    public @ResponseBody ResponseEntity<List<ElasticQueryServiceResponseModel>> getAllDocuments() {
        List<ElasticQueryServiceResponseModel> documents = elasticQueryService.getAllDocuments();
        LOG.info("Retrieving all documents. Total documents found: {}", documents.size());
        return ResponseEntity.ok(documents);
    }

    @GetMapping("v1/{id}")
    public @ResponseBody ResponseEntity<ElasticQueryServiceResponseModel> getDocumentById(@PathVariable @NotEmpty String id) {
        LOG.info("Retrieving document with id: {}", id);
        ElasticQueryServiceResponseModel document = elasticQueryService.getDocumentById(id);
        if (document == null) {
            LOG.warn("Document with id: {} not found", id);
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(document);
    }

    @GetMapping("v2/{id}")
    public @ResponseBody ResponseEntity<ElasticQueryServiceResponseModelv2> getDocumentByIdv2(@PathVariable @NotEmpty String id) {
        LOG.info("Retrieving document with id: {}", id);
        ElasticQueryServiceResponseModel document = elasticQueryService.getDocumentById(id);
        if (document == null) {
            LOG.warn("Document with id: {} not found", id);
            return ResponseEntity.notFound().build();
        }
        ElasticQueryServiceResponseModelv2 documentv2 = getV2Model(document);
        return ResponseEntity.ok(documentv2);
    }

    private ElasticQueryServiceResponseModelv2 getV2Model(ElasticQueryServiceResponseModel document) {
        ElasticQueryServiceResponseModelv2 responseModelv2 = ElasticQueryServiceResponseModelv2.builder()
                .id(Long.parseLong(document.getId()))
                .text(document.getText())
                .userId(document.getUserId())
                .createdAt(document.getCreatedAt())
                .build();
        responseModelv2.add(document.getLinks());
        return responseModelv2;
    }

    @PostMapping("v1/get-document-by-text")
    public @ResponseBody ResponseEntity<List<ElasticQueryServiceResponseModel>> getDocumentByText(@RequestBody @Valid ElasticQueryServiceRequestModel elasticQueryServiceRequestModel) {
        List<ElasticQueryServiceResponseModel> documents = elasticQueryService.getDocumentsByText(elasticQueryServiceRequestModel.getText());
        LOG.info("Retrieving documents with text: {}. Total documents found: {}", elasticQueryServiceRequestModel.getText(), documents.size());
        return ResponseEntity.ok(documents);
    }
}
