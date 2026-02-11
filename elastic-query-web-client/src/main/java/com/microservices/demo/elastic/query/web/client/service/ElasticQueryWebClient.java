package com.microservices.demo.elastic.query.web.client.service;



import com.microservices.demo.elastic.query.web.client.common.api.model.ElasticQueryWebClientRequestModel;
import com.microservices.demo.elastic.query.web.client.common.api.model.ElasticQueryWebClientResponseModel;

import java.util.List;

public interface ElasticQueryWebClient {
    List<ElasticQueryWebClientResponseModel> getDataByText(ElasticQueryWebClientRequestModel requestModel);
}
