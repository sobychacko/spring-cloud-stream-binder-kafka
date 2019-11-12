/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.streams.properties;

import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;

/**
 * Extended properties for Kafka Streams consumer.
 *
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
public class KafkaStreamsConsumerProperties extends KafkaConsumerProperties {

	private String applicationId;

	/**
	 * Key serde specified per binding.
	 */
	private String keySerde;

	/**
	 * Value serde specified per binding.
	 */
	private String valueSerde;

	/**
	 * Materialized as a KeyValueStore.
	 */
	private String materializedAs;

	/**
	 * Enumeration for various Serde errors.
	 */
	public enum DeserializationExceptionHandler {

		/**
		 * Deserialization error handler with log and continue.
		 * See {@link org.apache.kafka.streams.errors.LogAndContinueExceptionHandler}
		 */
		logAndContinue,
		/**
		 * Deserialization error handler with log and fail.
		 * See {@link org.apache.kafka.streams.errors.LogAndFailExceptionHandler}
		 */
		logAndFail,
		/**
		 * Deserialization error handler with DLQ send.
		 * See {@link org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler}
		 */
		sendToDlq

	}

	private DeserializationExceptionHandler deserializationExceptionHandler;

	/**
	 * {@link org.apache.kafka.streams.processor.TimestampExtractor} bean name to use for this consumer.
	 */
	private String timestampExtractorBeanName;

	public String getApplicationId() {
		return this.applicationId;
	}

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}

	public String getKeySerde() {
		return this.keySerde;
	}

	public void setKeySerde(String keySerde) {
		this.keySerde = keySerde;
	}

	public String getValueSerde() {
		return this.valueSerde;
	}

	public void setValueSerde(String valueSerde) {
		this.valueSerde = valueSerde;
	}

	public String getMaterializedAs() {
		return this.materializedAs;
	}

	public void setMaterializedAs(String materializedAs) {
		this.materializedAs = materializedAs;
	}

	public String getTimestampExtractorBeanName() {
		return timestampExtractorBeanName;
	}

	public void setTimestampExtractorBeanName(String timestampExtractorBeanName) {
		this.timestampExtractorBeanName = timestampExtractorBeanName;
	}

	public DeserializationExceptionHandler getDeserializationExceptionHandler() {
		return deserializationExceptionHandler;
	}

	public void setDeserializationExceptionHandler(DeserializationExceptionHandler deserializationExceptionHandler) {
		this.deserializationExceptionHandler = deserializationExceptionHandler;
	}
}
