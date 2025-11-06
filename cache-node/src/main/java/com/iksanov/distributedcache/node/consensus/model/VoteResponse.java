package com.iksanov.distributedcache.node.consensus.model;

public record VoteResponse(long term, boolean voteGranted) {}
