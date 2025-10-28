package com.iksanov.distributedcache.common.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for {@link HashUtils}.
 * Ensures hash stability, non-negativity, and reasonable distribution properties.
 */
class HashUtilsTest {

    @Test
    @DisplayName("hash() should always return the same value for the same input")
    void testHashIsDeterministic() {
        String key = "user:123";
        int hash1 = HashUtils.hash(key);
        int hash2 = HashUtils.hash(key);
        int hash3 = HashUtils.hash(key);

        assertEquals(hash1, hash2, "Hash must be stable across calls");
        assertEquals(hash1, hash3, "Hash must be stable across multiple invocations");
    }

    @Test
    @DisplayName("hash() should produce different results for different inputs")
    void testDifferentInputsProduceDifferentHashes() {
        int hash1 = HashUtils.hash("alpha");
        int hash2 = HashUtils.hash("beta");

        assertNotEquals(hash1, hash2, "Different strings should have different hash values");
    }

    @Test
    @DisplayName("hash() result must always be non-negative")
    void testHashIsNonNegative() {
        int hash = HashUtils.hash("key-123");
        assertTrue(hash >= 0, "Hash must be non-negative");

        int hashZero = HashUtils.hash("0");
        assertTrue(hashZero >= 0, "Hash for '0' should also be non-negative");
    }

    @Test
    @DisplayName("hash() should be sensitive to small input changes")
    void testHashSensitivityToSmallChanges() {
        String base = "user:100";
        String changed = "user:101";

        int hash1 = HashUtils.hash(base);
        int hash2 = HashUtils.hash(changed);

        assertNotEquals(hash1, hash2, "Small input changes should produce different hashes");
    }

    @Test
    @DisplayName("hash() should throw IllegalArgumentException for null input")
    void testHashNullThrows() {
        assertThrows(IllegalArgumentException.class, () -> HashUtils.hash(null));
    }

    @Test
    @DisplayName("hash() should produce reasonably uniform distribution for many keys")
    void testHashDistributionUniformity() {
        int sampleSize = 100_000;
        int buckets = 100;
        int[] counts = new int[buckets];

        for (int i = 0; i < sampleSize; i++) {
            int hash = HashUtils.hash("key-" + i);
            int bucket = hash % buckets;
            counts[bucket]++;
        }

        double avg = sampleSize / (double) buckets;
        double maxDeviation = 0;
        for (int count : counts) {
            double deviation = Math.abs(count - avg) / avg;
            maxDeviation = Math.max(maxDeviation, deviation);
        }

        assertTrue(maxDeviation < 0.25,
                "Distribution across buckets is too uneven, max deviation=" + maxDeviation);
    }

    @Test
    @DisplayName("hash() should handle large random keys without collisions in a small set")
    void testNoCollisionsInSmallSet() {
        Set<Integer> hashes = new HashSet<>();
        Random rnd = new Random(42);

        for (int i = 0; i < 10_000; i++) {
            String key = "rand-" + rnd.nextInt(1_000_000);
            int h = HashUtils.hash(key);
            assertTrue(h >= 0);
            hashes.add(h);
        }

        double uniqueRatio = hashes.size() / 10_000.0;
        assertTrue(uniqueRatio > 0.99,
                "Too many collisions: only " + hashes.size() + " unique hashes out of 10k");
    }
}
