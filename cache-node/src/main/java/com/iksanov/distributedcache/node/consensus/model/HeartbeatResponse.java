package com.iksanov.distributedcache.node.consensus.model;

public record HeartbeatResponse(long term, boolean success) {}
