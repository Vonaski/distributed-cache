package com.iksanov.distributedcache.node.consensus;

public interface LeaderChangeListener {
    void onBecomeLeader();
    void onLoseLeadership();
    void onLeaderChange(String newLeader);
}
