package com.microservices.demo.reactive.elastic.query.service.business.impl;

import com.microservices.demo.config.ElasticConfigData;
import com.microservices.demo.reactive.elastic.query.service.business.ElasticQueryService;
import com.microservices.demo.reactive.elastic.query.service.business.ReactiveElasticQueryClient;
import com.microservices.demo.elastic.query.service.common.model.ElasticQueryServiceResponseModel;
import com.microservices.demo.elastic.query.service.common.transformer.ElasticToResponseModelTransformer;
import com.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
public class TwitterElasticQueryService implements ElasticQueryService {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterReactiveElasticQueryClient.class);
    private final ReactiveElasticQueryClient<TwitterIndexModel> twitterReactiveElasticQueryClient;
    private final ElasticToResponseModelTransformer elasticToResponseModelTransformer;


    public TwitterElasticQueryService(ReactiveElasticQueryClient<TwitterIndexModel> twitterReactiveElasticQueryClient, ElasticToResponseModelTransformer elasticToResponseModelTransformer, ElasticConfigData elasticConfigData) {
        this.twitterReactiveElasticQueryClient = twitterReactiveElasticQueryClient;
        this.elasticToResponseModelTransformer = elasticToResponseModelTransformer;
    }

    @Override
    public Flux<ElasticQueryServiceResponseModel> getDocumentByText(String text) {
       LOG.info("Get document by text: {}", text);
         return twitterReactiveElasticQueryClient.getIndexModelByText(text)
                .map(elasticToResponseModelTransformer::getResponseModel);
    }
}
