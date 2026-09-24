package com.broker.dsl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Declarative Infrastructure DSL Specification for Cluster Topology.
 * Inspired by Terraform and Kubernetes declarative controllers.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterTopologySpec {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BrokerSpec {
        @JsonProperty("id")
        private String id;

        @JsonProperty("host")
        private String host;

        @JsonProperty("port")
        private int port;

        @JsonProperty("zone")
        private String zone;

        public BrokerSpec() {}

        public BrokerSpec(String id, String host, int port, String zone) {
            this.id = id;
            this.host = host;
            this.port = port;
            this.zone = zone;
        }

        public String getId() { return id; }
        public String getHost() { return host; }
        public int getPort() { return port; }
        public String getZone() { return zone; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PlacementPolicySpec {
        @JsonProperty("minDistinctZones")
        private int minDistinctZones = 1;

        public PlacementPolicySpec() {}
        public PlacementPolicySpec(int minDistinctZones) {
            this.minDistinctZones = minDistinctZones;
        }

        public int getMinDistinctZones() { return minDistinctZones; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TopicSpec {
        @JsonProperty("name")
        private String name;

        @JsonProperty("partitions")
        private int partitions = 1;

        @JsonProperty("replicationFactor")
        private int replicationFactor = 1;

        @JsonProperty("placementPolicy")
        private PlacementPolicySpec placementPolicy = new PlacementPolicySpec(1);

        @JsonProperty("config")
        private Map<String, String> config = Collections.emptyMap();

        public TopicSpec() {}

        public TopicSpec(String name, int partitions, int replicationFactor, PlacementPolicySpec placementPolicy) {
            this.name = name;
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.placementPolicy = placementPolicy != null ? placementPolicy : new PlacementPolicySpec(1);
        }

        public String getName() { return name; }
        public int getPartitions() { return partitions; }
        public int getReplicationFactor() { return replicationFactor; }
        public PlacementPolicySpec getPlacementPolicy() { return placementPolicy; }
        public Map<String, String> getConfig() { return config; }
    }

    @JsonProperty("version")
    private String version = "v1alpha1";

    @JsonProperty("brokers")
    private List<BrokerSpec> brokers = Collections.emptyList();

    @JsonProperty("topics")
    private List<TopicSpec> topics = Collections.emptyList();

    public ClusterTopologySpec() {}

    public ClusterTopologySpec(List<BrokerSpec> brokers, List<TopicSpec> topics) {
        this.brokers = brokers;
        this.topics = topics;
    }

    public String getVersion() { return version; }
    public List<BrokerSpec> getBrokers() { return brokers; }
    public List<TopicSpec> getTopics() { return topics; }
}
