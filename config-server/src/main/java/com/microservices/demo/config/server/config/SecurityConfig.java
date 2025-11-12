package com.microservices.demo.config.server.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

@Configuration
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .csrf(csrf -> csrf.disable()) //postman fix
                .authorizeHttpRequests(authorize -> authorize
                       // .requestMatchers(new AntPathRequestMatcher("/encrypt/**"),
                         //       new AntPathRequestMatcher("/decrypt/**"),
                          //      new AntPathRequestMatcher("/config-client/twitter_to_kafka/**"),
                          //      new AntPathRequestMatcher("/twitter-to-kafka-service,config-client/**"))
                        .anyRequest()
                        .permitAll()
                )
                .httpBasic(Customizer.withDefaults());
        return http.build();
    }
}
