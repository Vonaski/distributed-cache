package com.iksanov.distributedcache.node.consensus.transport;

public enum MessageType {
    APPEND_ENTRIES,
    APPEND_ENTRIES_RESPONSE,
    REQUEST_VOTE,
    REQUEST_VOTE_RESPONSE,
    HEARTBEAT,
    NOT_LEADER
}
