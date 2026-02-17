package com.microservices.demo.elastic.query.service.api;

import com.microservices.demo.elastic.query.service.business.ElasticQueryService;

import com.microservices.demo.elastic.query.service.common.model.ElasticQueryServiceRequestModel;
import com.microservices.demo.elastic.query.service.common.model.ElasticQueryServiceResponseModel;
import com.microservices.demo.elastic.query.service.model.ElasticQueryServiceResponseModelV2;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@PreAuthorize("isAuthenticated()")
@RestController
@RequestMapping(value="/documents",produces = "application/vnd.api-v1+json")
public class ElasticDocumentController {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticDocumentController.class);
    private final ElasticQueryService elasticQueryService;

    @Value("${server.port}")
    private String port;
    public ElasticDocumentController(ElasticQueryService elasticQueryService) {
        this.elasticQueryService = elasticQueryService;
    }


    @Operation(summary = "Get all elastic documents", description = "Retrieve all documents from the Elasticsearch index.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved all documents", content={
                    @Content(mediaType = "application/vnd.api-v1+json",
                            schema = @Schema(implementation = ElasticQueryServiceResponseModel.class))

            }),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "400", description = "Not found. Bad request")
    })
    @GetMapping("/")
    public @ResponseBody ResponseEntity<List<ElasticQueryServiceResponseModel>> getAllDocuments() {
        List<ElasticQueryServiceResponseModel> documents = elasticQueryService.getAllDocuments();
        LOG.info("Retrieving all documents. Total documents found: {}", documents.size());
        return ResponseEntity.ok(documents);
    }

    @Operation(summary = "Get elastic document by ID", description = "Retrieve a document from the Elasticsearch index by its ID.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved the document", content={
                    @Content(mediaType = "application/vnd.api-v1+json",
                            schema = @Schema(implementation = ElasticQueryServiceResponseModel.class))

            }),
            @ApiResponse(responseCode = "404", description = "Document not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "400", description = "Bad request")
    })
    @GetMapping("/{id}")
    public @ResponseBody ResponseEntity<ElasticQueryServiceResponseModel> getDocumentById(@PathVariable @NotEmpty String id) {
        LOG.info("Retrieving document with id: {}", id);
        ElasticQueryServiceResponseModel document = elasticQueryService.getDocumentById(id);
        if (document == null) {
            LOG.warn("Document with id: {} not found", id);
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(document);
    }

    @Operation(summary = "Get elastic document by ID - V2", description = "Retrieve a document from the Elasticsearch index by its ID. This is version 2 of the API with additional fields.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved the document", content={
                    @Content(mediaType = "application/vnd.api-v2+json",
                            schema = @Schema(implementation = ElasticQueryServiceResponseModelV2.class))

            }),
            @ApiResponse(responseCode = "404", description = "Document not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "400", description = "Bad request")
    })
    @GetMapping(value="/{id}",produces = "application/vnd.api-v2+json")
    public @ResponseBody ResponseEntity<ElasticQueryServiceResponseModelV2> getDocumentByIdV2(@PathVariable @NotEmpty String id) {
        LOG.info("Retrieving document with id: {}", id);
        ElasticQueryServiceResponseModel document = elasticQueryService.getDocumentById(id);
        if (document == null) {
            LOG.warn("Document with id: {} not found", id);
            return ResponseEntity.notFound().build();
        }
        ElasticQueryServiceResponseModelV2 documentV2 = getV2Model(document);
        return ResponseEntity.ok(documentV2);
    }






    @PreAuthorize("hasRole('APP_USER_ROLE') || hasAuthority('SCOPE_APP_USER_ROLE')")
    @Operation(summary = "Get elastic documents by text", description = "Retrieve documents from the Elasticsearch index that match the given text.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved documents", content={
                    @Content(mediaType = "application/vnd.api-v1+json",
                            schema = @Schema(implementation = ElasticQueryServiceResponseModel.class))

            }),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "400", description = "Bad request")
    })
    @PostMapping("/get-document-by-text")
    public @ResponseBody ResponseEntity<List<ElasticQueryServiceResponseModel>> getDocumentByText(@RequestBody @Valid ElasticQueryServiceRequestModel elasticQueryServiceRequestModel) {
        List<ElasticQueryServiceResponseModel> documents = elasticQueryService.getDocumentsByText(elasticQueryServiceRequestModel.getText());
        LOG.info("Retrieving documents with text: {}. Total documents found: {}, on port:{}", elasticQueryServiceRequestModel.getText(), documents.size(), port);
        return ResponseEntity.ok(documents);
    }

    private ElasticQueryServiceResponseModelV2 getV2Model(ElasticQueryServiceResponseModel document) {
        ElasticQueryServiceResponseModelV2 responseModelV2 = ElasticQueryServiceResponseModelV2.builder()
                .id(Long.parseLong(document.getId()))
                .userId(document.getUserId())
                .text(document.getText())
                .text2("This is additional field in V2")
                .build();
        responseModelV2.add(document.getLinks());
        return responseModelV2;
    }

    @PostMapping("/get-document-by-text-test")
    public @ResponseBody ResponseEntity<List<ElasticQueryServiceResponseModel>>
    getDocumentByTextTest(@RequestHeader(value = "Authorization", required = false) String authorizationHeader,@RequestBody @Valid ElasticQueryServiceRequestModel elasticQueryServiceRequestModel) {
        List<ElasticQueryServiceResponseModel> documents = elasticQueryService.getDocumentsByText(elasticQueryServiceRequestModel.getText());
        LOG.info("Retrieving documents with text: {}. Total documents found: {}, on port:{}", elasticQueryServiceRequestModel.getText(), documents.size(), port);
        return ResponseEntity.ok(documents);
    }




}
