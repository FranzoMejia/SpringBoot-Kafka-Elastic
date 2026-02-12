package com.microservices.demo.reactive.elastic.query.service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;




@Configuration
public class WebSecurityConfig {

    @Bean
    public SecurityWebFilterChain securityFilterChain(ServerHttpSecurity http) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .authorizeExchange(authorize -> authorize
                        .anyExchange()
                        .permitAll()
                )
                .httpBasic(Customizer.withDefaults());
        return http.build();
    }

}
