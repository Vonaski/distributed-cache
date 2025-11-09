package com.iksanov.distributedcache.node.consensus.model;

public record AppendEntriesResponse(long term, boolean success, long matchIndex) {
}