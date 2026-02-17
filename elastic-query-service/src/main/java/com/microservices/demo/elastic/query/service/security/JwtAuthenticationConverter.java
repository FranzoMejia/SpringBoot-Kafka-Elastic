package com.microservices.demo.elastic.query.service.security;

import com.microservices.demo.config.ElasticConfigData;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

@Component
public class JwtAuthenticationConverter implements Converter<Jwt, AbstractAuthenticationToken> {
    private static final String REALM_ACCESS_CLAIM="realm_access";
    private static final String ROLES_CLAIM="roles";
    private static final String SCOPE_CLAIM="scope";
    private static final String USERNAME_CLAIM="preferred_username";
    private static final String DEFAULT_ROLE_PREFIX="ROLE_";
    private static final String DEFAULT_SCOPE_PREFIX="SCOPE_";
    private static final String SCOPE_SEPARATOR=" ";
    private final JwtGrantedAuthoritiesConverter jwtGrantedAuthoritiesConverter=new JwtGrantedAuthoritiesConverter();
    private final ElasticConfigData elasticConfigData;

    public JwtAuthenticationConverter(ElasticConfigData elasticConfigData) {
        this.elasticConfigData = elasticConfigData;
    }

    @Override
    public AbstractAuthenticationToken convert(Jwt jwt) {
        Collection<GrantedAuthority> authorities = Stream.concat(jwtGrantedAuthoritiesConverter.convert(jwt).stream(), extractResourceRoles(jwt).stream())
                .toList();
        return new JwtAuthenticationToken(jwt, authorities, getPrincipalClaimName(jwt));
    }

    private String getPrincipalClaimName(Jwt jwt) {
        String claimName = JwtClaimNames.SUB;
        if(jwt.getClaims() != null) {
            claimName = USERNAME_CLAIM;
        }
        return claimName;
    }

    private Collection<? extends GrantedAuthority> extractResourceRoles(Jwt jwt) {

        if(jwt.getClaimAsMap(REALM_ACCESS_CLAIM) == null || jwt.getClaimAsMap(REALM_ACCESS_CLAIM).get(ROLES_CLAIM) == null) {
            return Stream.<GrantedAuthority>empty().toList();
        }
        Map<String,Object> realmAccess = jwt.getClaimAsMap(REALM_ACCESS_CLAIM);
        //Map<String,Object> realmRoles = (Map<String, Object>) realmAccess.get(ROLES_CLAIM);
        Collection<String> resourceRoles = (Collection<String>) realmAccess.get(ROLES_CLAIM);
        return resourceRoles.stream().map(role -> DEFAULT_ROLE_PREFIX + role.toUpperCase()).map(SimpleGrantedAuthority::new).toList();
    }
}
