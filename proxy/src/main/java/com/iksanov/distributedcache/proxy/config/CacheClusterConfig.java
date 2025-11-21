package com.iksanov.distributedcache.proxy.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "cache.cluster")
public class CacheClusterConfig {

    private String nodes;
    private int virtualNodes = 150;
    private ConnectionConfig connection = new ConnectionConfig();

    public List<String> getNodes() {
        if (nodes == null || nodes.isBlank()) return new ArrayList<>();
        return Arrays.stream(nodes.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    @Getter
    @Setter
    public static class ConnectionConfig {
        private int timeoutMillis = 3000;
        private int maxRetries = 2;
        private int poolSize = 10;
    }
}


