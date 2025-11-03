package com.iksanov.distributedcache.common.util;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;

public final class HashUtils {

    private HashUtils() {}

    public static int hash(String key) {
        if (key == null) throw new IllegalArgumentException("key is null");

        return Hashing.murmur3_32_fixed()
                .hashString(key, StandardCharsets.UTF_8)
                .asInt() & 0x7FFFFFFF;
    }
}
