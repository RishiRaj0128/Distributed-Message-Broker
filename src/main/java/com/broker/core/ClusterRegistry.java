package com.broker.core;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cluster registry maintaining active broker nodes and configured topics.
 */
public class ClusterRegistry {
    private final Map<String, BrokerNode> brokers = new ConcurrentHashMap<>();
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final OffsetTracker offsetTracker = new OffsetTracker();

    public void registerBroker(BrokerNode broker) {
        brokers.put(broker.getId(), broker);
    }

    public void unregisterBroker(String brokerId) {
        brokers.remove(brokerId);
    }

    public BrokerNode getBroker(String brokerId) {
        return brokers.get(brokerId);
    }

    public Collection<BrokerNode> getAllBrokers() {
        return Collections.unmodifiableCollection(brokers.values());
    }

    public void registerTopic(Topic topic) {
        topics.put(topic.getName(), topic);
    }

    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }

    public Collection<Topic> getAllTopics() {
        return Collections.unmodifiableCollection(topics.values());
    }

    public OffsetTracker getOffsetTracker() {
        return offsetTracker;
    }

    public void clear() {
        brokers.clear();
        topics.clear();
        offsetTracker.clear();
    }
}
