package com.microservices.demo.elastic.query.service.business.impl;

import com.microservices.demo.config.ElasticQueryServiceConfigData;
import com.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import com.microservices.demo.elastic.query.client.service.ElasticQueryClient;
import com.microservices.demo.elastic.query.service.QueryType;
import com.microservices.demo.elastic.query.service.business.ElasticQueryService;

import com.microservices.demo.elastic.query.service.common.model.ElasticQueryServiceResponseModel;
import com.microservices.demo.elastic.query.service.model.ElasticQueryServiceAnalyticsResponseModel;
import com.microservices.demo.elastic.query.service.model.ElasticQueryServiceWordCountResponseModel;
import com.microservices.demo.elastic.query.service.model.assembler.ElasticQueryServiceResponseModelAssembler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
public class TwitterElasticQueryService implements ElasticQueryService {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterElasticQueryService.class);
    private final ElasticQueryServiceResponseModelAssembler elasticQueryServiceResponseModelAssembler;
    private final ElasticQueryClient<TwitterIndexModel> elasticQueryClient;
    private final ElasticQueryServiceConfigData elasticQueryServiceConfigData;
    private final WebClient.Builder webClientBuilder;
    public TwitterElasticQueryService(ElasticQueryServiceResponseModelAssembler elasticQueryServiceResponseModelAssembler, ElasticQueryClient<TwitterIndexModel> elasticQueryClient, ElasticQueryServiceConfigData elasticQueryServiceConfigData, @Qualifier("webClientBuilder") WebClient.Builder webClientBuilder) {
        this.elasticQueryServiceResponseModelAssembler = elasticQueryServiceResponseModelAssembler;
        this.elasticQueryClient = elasticQueryClient;
        this.elasticQueryServiceConfigData = elasticQueryServiceConfigData;
        this.webClientBuilder = webClientBuilder;
    }
    @Override
    public ElasticQueryServiceResponseModel getDocumentById(String id) {
        LOG.info("Retrieving document with id: {}", id);
        return elasticQueryServiceResponseModelAssembler.toModel(elasticQueryClient.getIndexModelById(id));
    }

    @Override
    public ElasticQueryServiceAnalyticsResponseModel getDocumentsByText(String text,String accessToken) {
        LOG.info("Retrieving documents with text: {}", text);
        List<ElasticQueryServiceResponseModel> elasticQueryServiceResponseModels =
                elasticQueryServiceResponseModelAssembler.toModels(elasticQueryClient.getIndexModelsByText(text));
        return ElasticQueryServiceAnalyticsResponseModel.builder()
                .queryResponseModels(elasticQueryServiceResponseModels)
                .wordCount(getWordCount(text,accessToken))
                .build();

    }
    @Override
    public List<ElasticQueryServiceResponseModel> getAllDocuments() {
        LOG.info("Retrieving all documents");
        return elasticQueryServiceResponseModelAssembler.toModels(elasticQueryClient.getAllIndexModels());
    }

    private Long getWordCount(String text, String accessToken) {
        if(QueryType.KAFKA_STATE_STORE.getType().equals(elasticQueryServiceConfigData.getWebClient().getQueryType())){
            return getFromKafkaStateStore(text,accessToken).getWordCount();
        }
        return 0L;
    }

    private ElasticQueryServiceWordCountResponseModel getFromKafkaStateStore(String text, String accessToken) {
        ElasticQueryServiceConfigData.Query queryFromKafkaStateStore =
                elasticQueryServiceConfigData.getQueryFromKafkaStateStore();
        return retrieveResposeModel(text,accessToken,queryFromKafkaStateStore);
    }

    private ElasticQueryServiceWordCountResponseModel retrieveResposeModel(String text, String accessToken, ElasticQueryServiceConfigData.Query queryFromKafkaStateStore) {
        return webClientBuilder
                .build()
                .method(HttpMethod.valueOf(queryFromKafkaStateStore.getMethod()))
                .uri(queryFromKafkaStateStore.getUri(), uriBuilder -> uriBuilder.build(text))
                .headers(h-> h.setBearerAuth(accessToken))
                .accept(MediaType.valueOf(queryFromKafkaStateStore.getAccept()))
                .retrieve()
                .onStatus(
                        s->s.equals(HttpStatus.UNAUTHORIZED),
                        clientResponse -> Mono.just(new BadCredentialsException("Unauthorized access to Kafka State Store")))
                .onStatus(
                        HttpStatusCode::is4xxClientError,
                        clientResponse -> Mono.just(new RuntimeException("Client error while accessing Kafka State Store: " + clientResponse.statusCode()))
                )
                .onStatus(
                        HttpStatusCode::is5xxServerError,
                        clientResponse -> Mono.just(new RuntimeException("Server error while accessing Kafka State Store: " + clientResponse.statusCode()))
                )
                .bodyToMono(ElasticQueryServiceWordCountResponseModel.class)
                .log()
                .block();

    }


}
