package com.iksanov.distributedcache.proxy.controller;

import com.iksanov.distributedcache.proxy.service.CacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/cache")
public class CacheController {

    private static final Logger log = LoggerFactory.getLogger(CacheController.class);
    private final CacheService cacheService;

    public CacheController(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @GetMapping("/{key}")
    public ResponseEntity<Map<String, String>> get(@PathVariable String key) {
        log.debug("GET request for key: {}", key);
        String value = cacheService.get(key);
        if (value == null) {
            return ResponseEntity
                    .status(HttpStatus.NOT_FOUND)
                    .body(Map.of("key", key, "message", "Key not found"));
        }
        return ResponseEntity.ok(Map.of("key", key, "value", value));
    }

    @PutMapping("/{key}")
    public ResponseEntity<Map<String, String>> set(@PathVariable String key, @RequestBody Map<String, String> body) {
        log.debug("PUT request for key: {}", key);
        
        String value = body.get("value");
        if (value == null) {
            return ResponseEntity
                    .badRequest()
                    .body(Map.of("error", "Value is required in request body"));
        }
        cacheService.set(key, value);
        return ResponseEntity.ok(Map.of("key", key, "message", "Value set successfully"));
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<Map<String, String>> delete(@PathVariable String key) {
        log.debug("DELETE request for key: {}", key);
        
        cacheService.delete(key);
        
        return ResponseEntity.ok(Map.of("key", key, "message", "Key deleted successfully"));
    }
}