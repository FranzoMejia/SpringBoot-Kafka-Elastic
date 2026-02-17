package com.microservices.demo.elastic.query.service.config;

import com.microservices.demo.elastic.query.service.security.JwtAuthenticationConverter;
import com.microservices.demo.elastic.query.service.security.TwitterQueryUserDetailsService;
import com.microservices.demo.elastic.query.service.security.TwitterQueryUserJwtConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.security.oauth2.resource.OAuth2ResourceServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.access.expression.method.DefaultMethodSecurityExpressionHandler;
import org.springframework.security.access.expression.method.MethodSecurityExpressionHandler;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.web.SecurityFilterChain;


import java.util.Arrays;

@Configuration
@EnableWebSecurity
public class WebSecurityConfig {

    @Value("${security.paths-to-ignore}")
    private String[] pathsToIgnore;


    private final OAuth2ResourceServerProperties oAuth2ResourceServerProperties;
    private final TwitterQueryUserDetailsService twitterQueryUserDetailsService;
    private final JwtAuthenticationConverter jwtAuthenticationConverter;




    public WebSecurityConfig(OAuth2ResourceServerProperties oAuth2ResourceServerProperties, TwitterQueryUserDetailsService twitterQueryUserDetailsService, JwtAuthenticationConverter jwtAuthenticationConverter) {
        this.oAuth2ResourceServerProperties = oAuth2ResourceServerProperties;
        this.twitterQueryUserDetailsService = twitterQueryUserDetailsService;
        this.jwtAuthenticationConverter = jwtAuthenticationConverter;
    }
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                // Disable CSRF for stateless APIs
                .csrf(AbstractHttpConfigurer::disable)
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(pathsToIgnore).permitAll()// Allow public access to certain endpoints
                        .anyRequest().authenticated() // Secure all other endpoints
                )
                .sessionManagement(session -> session
                        .sessionCreationPolicy(SessionCreationPolicy.STATELESS) // Set session policy to STATELESS
                )

                .oauth2ResourceServer(oauth2 -> oauth2
                        .jwt(jwt -> jwt.jwtAuthenticationConverter(twitterQueryUserJwtConverter()))); // Enable JWT authentication with defaults
        return http.build();

    }

    @Bean
    Converter<Jwt, ? extends AbstractAuthenticationToken> twitterQueryUserJwtConverter() {//
         return new TwitterQueryUserJwtConverter(twitterQueryUserDetailsService);
    }
}
