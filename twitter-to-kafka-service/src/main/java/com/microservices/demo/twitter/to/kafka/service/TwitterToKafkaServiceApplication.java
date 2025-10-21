package com.microservices.demo.twitter.to.kafka.service;

import com.microservices.demo.twitter.to.kafka.service.config.TwitterToKakfaConfigData;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);
    private final TwitterToKakfaConfigData twitterToKakfaConfigData;

    private final StreamRunner streamRunner;

    public TwitterToKafkaServiceApplication(TwitterToKakfaConfigData configData, StreamRunner streamRunner) {
        this.twitterToKakfaConfigData = configData;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        org.springframework.boot.SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("TwitterToKafkaServiceApplication started with keywords: {}", twitterToKakfaConfigData.getTwitterKeywords());
        LOG.info(twitterToKakfaConfigData.getWelcomeMessage());
        streamRunner.start();
    }
}
