package com.microservices.demo.kafka.streams.service.api;

import com.microservices.demo.kafka.streams.service.model.KafkaStreamsResponseModel;
import com.microservices.demo.kafka.streams.service.runner.StreamsRunner;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.validation.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@PreAuthorize("isAuthenticated()")
@RestController
@RequestMapping(value = "/" , produces = "application/vnd.api.v1+json")
public class KafkaStreamsController {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsController.class);
    private final StreamsRunner<String, Long> kafkaStreamsRunner;

    public KafkaStreamsController(StreamsRunner<String, Long> kafkaStreamsRunner) {
        this.kafkaStreamsRunner = kafkaStreamsRunner;
    }

    @GetMapping("get-word-count-by-word/{word}")
    @Operation(summary = "Get word count by word",
            description = "Get word count by word",
            tags = {"Kafka Streams"})
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Successfully retrieved word count for the given word"),
            @ApiResponse(responseCode = "400",
                    description = "Invalid input, such as empty word"),
            @ApiResponse(responseCode = "404",
                    description = "Word not found in the stream"),
            @ApiResponse(responseCode = "500",
                    description = "Internal server error while processing the request")
    })
    public @ResponseBody ResponseEntity<KafkaStreamsResponseModel> getWordCountByWord(@PathVariable @NotEmpty String word) {
        LOG.info("Getting word count for word: {}", word);
        Long wordCount = kafkaStreamsRunner.getValueByKey(word);
        KafkaStreamsResponseModel responseModel = KafkaStreamsResponseModel.builder()
                .word(word)
                .WordCount(wordCount)
                .build();
        return ResponseEntity.ok(responseModel);
    }
}
