package com.iksanov.distributedcache.node.consensus.model;

public record HeartbeatRequest(long term, String leaderId) {
}
