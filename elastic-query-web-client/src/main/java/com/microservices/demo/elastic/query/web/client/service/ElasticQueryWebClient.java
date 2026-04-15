package com.microservices.demo.elastic.query.web.client.service;



import com.microservices.demo.elastic.query.web.client.common.api.model.ElasticQueryWebClientAnalyticsResponseModel;
import com.microservices.demo.elastic.query.web.client.common.api.model.ElasticQueryWebClientRequestModel;
import com.microservices.demo.elastic.query.web.client.common.api.model.ElasticQueryWebClientResponseModel;

import java.util.List;

public interface ElasticQueryWebClient {
    ElasticQueryWebClientAnalyticsResponseModel getDataByText(ElasticQueryWebClientRequestModel requestModel);
}
