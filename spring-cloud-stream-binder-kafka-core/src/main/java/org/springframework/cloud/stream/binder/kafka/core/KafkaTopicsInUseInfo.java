package org.springframework.cloud.stream.binder.kafka.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.PartitionInfo;

/**
 * @author Soby Chacko
 */
public class KafkaTopicsInUseInfo {

	private Map<String, Collection<PartitionInfo>> topicsInUse = new HashMap<>();

	public Map<String, Collection<PartitionInfo>> getTopicsInUse() {
		return topicsInUse;
	}

	public void addTopic(String topic, Collection<PartitionInfo> partitions) {
		this.topicsInUse.put(topic, partitions);
	}
}
