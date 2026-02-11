package com.microservices.demo.reactive.elastic.query.service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.server.SecurityWebFilterChain;

import static org.springframework.security.config.Customizer.withDefaults;


@Configuration
@EnableWebFluxSecurity
public class WebSecurityConfig {

    @Bean
    public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        http
                .csrf(ServerHttpSecurity.CsrfSpec::disable) // Disable CSRF for simpler configurations, enable as needed
                .authorizeExchange(exchanges -> exchanges
                        .pathMatchers("/public/**").permitAll() // Allow access to public endpoints
                        .anyExchange().authenticated() // Require authentication for all other endpoints
                )
                .httpBasic(withDefaults()) // Enable HTTP Basic authentication
                .formLogin(withDefaults()); // Enable form login with default settings

        return http.build();
    }

    @Bean
    public MapReactiveUserDetailsService userDetailsService() {
        // Use PasswordEncoderFactories.createDelegatingPasswordEncoder() for secure password encoding
        PasswordEncoder encoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();

        UserDetails user = org.springframework.security.core.userdetails.User.builder()
                .username("user")
                .password(encoder.encode("password")) // Encode the password
                .roles("USER")
                .build();

        UserDetails admin = org.springframework.security.core.userdetails.User.builder()
                .username("admin")
                .password(encoder.encode("adminpass"))
                .roles("USER", "ADMIN")
                .build();

        return new MapReactiveUserDetailsService(user, admin);
    }
}
