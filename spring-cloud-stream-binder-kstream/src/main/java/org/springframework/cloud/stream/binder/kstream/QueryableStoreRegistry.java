package org.springframework.cloud.stream.binder.kstream;

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreType;

/**
 * @author Soby Chacko
 */
public class QueryableStoreRegistry {

	private final Set<KafkaStreams> kafkaStreams = new HashSet<>();

	public <T> T getQueryableStoreType(String storeName, QueryableStoreType<T> storeType) {

		for (KafkaStreams kafkaStream : kafkaStreams) {
			T store = kafkaStream.store(storeName, storeType);
			if (store != null) {
				return store;
			}
		}
		return null;
	}

	public void registerKafkaStreams(KafkaStreams kafkaStreams) {
		this.kafkaStreams.add(kafkaStreams);
	}
}
