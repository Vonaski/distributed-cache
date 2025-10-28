package com.iksanov.distributedcache.common.util;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

public final class HashUtils {

    private HashUtils() {}

    public static int hash(String key) {
        if (key == null) throw new IllegalArgumentException("key is null");
        CRC32 crc = new CRC32();
        crc.update(key.getBytes(StandardCharsets.UTF_8));
        long value = crc.getValue();
        return (int) (value & 0x7FFFFFFF);
    }
}
