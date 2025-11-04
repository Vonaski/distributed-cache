package com.iksanov.distributedcache.node.consensus.model;

public record VoteRequest(long term, String candidateId) {
}
