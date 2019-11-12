/*
 * Copyright 2018-2019 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;

/**
 * Kafka Streams binder configuration properties.
 *
 * @author Soby Chacko
 * @author Gary Russell
 */
public class KafkaStreamsBinderConfigurationProperties
		extends KafkaBinderConfigurationProperties {

	public KafkaStreamsBinderConfigurationProperties(KafkaProperties kafkaProperties) {
		super(kafkaProperties);
	}

	/**
	 * Enumeration for various Serde errors.
	 */
	@Deprecated
	public enum SerdeError {

		/**
		 * Deserialization error handler with log and continue.
		 */
		logAndContinue,
		/**
		 * Deserialization error handler with log and fail.
		 */
		logAndFail,
		/**
		 * Deserialization error handler with DLQ send.
		 */
		sendToDlq

	}

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

	private String applicationId;

	private StateStoreRetry stateStoreRetry = new StateStoreRetry();

	private Map<String, Functions> functions = new HashMap<>();

	public Map<String, Functions> getFunctions() {
		return functions;
	}

	public void setFunctions(Map<String, Functions> functions) {
		this.functions = functions;
	}

	public StateStoreRetry getStateStoreRetry() {
		return stateStoreRetry;
	}

	public void setStateStoreRetry(StateStoreRetry stateStoreRetry) {
		this.stateStoreRetry = stateStoreRetry;
	}

	public String getApplicationId() {
		return this.applicationId;
	}

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}

	/**
	 * {@link org.apache.kafka.streams.errors.DeserializationExceptionHandler} to use when
	 * there is a Serde error.
	 * {@link KafkaStreamsBinderConfigurationProperties.SerdeError} values are used to
	 * provide the exception handler on consumer binding.
	 */
	private KafkaStreamsBinderConfigurationProperties.SerdeError serdeError;

	private DeserializationExceptionHandler deserializationExceptionHandler;

	@Deprecated
	public KafkaStreamsBinderConfigurationProperties.SerdeError getSerdeError() {
		return this.serdeError;
	}

	@Deprecated
	public void setSerdeError(
			KafkaStreamsBinderConfigurationProperties.SerdeError serdeError) {
			this.serdeError = serdeError;
			if (serdeError == SerdeError.logAndContinue) {
				this.deserializationExceptionHandler = DeserializationExceptionHandler.logAndContinue;
			}
			else if (serdeError == SerdeError.logAndFail) {
				this.deserializationExceptionHandler = DeserializationExceptionHandler.logAndFail;
			}
			else if (serdeError == SerdeError.sendToDlq) {
				this.deserializationExceptionHandler = DeserializationExceptionHandler.sendToDlq;
			}
	}

	public DeserializationExceptionHandler getDeserializationExceptionHandler() {
		return deserializationExceptionHandler;
	}

	public void setDeserializationExceptionHandler(DeserializationExceptionHandler deserializationExceptionHandler) {
		this.deserializationExceptionHandler = deserializationExceptionHandler;
	}

	public static class StateStoreRetry {

		private int maxAttempts = 1;

		private long backoffPeriod = 1000;

		public int getMaxAttempts() {
			return maxAttempts;
		}

		public void setMaxAttempts(int maxAttempts) {
			this.maxAttempts = maxAttempts;
		}

		public long getBackoffPeriod() {
			return backoffPeriod;
		}

		public void setBackoffPeriod(long backoffPeriod) {
			this.backoffPeriod = backoffPeriod;
		}
	}

	public static class Functions {

		/**
		 * Function specific application id.
		 */
		private String applicationId;

		/**
		 * Funcion specific configuraiton to use.
		 */
		private Map<String, String> configuration;

		public String getApplicationId() {
			return applicationId;
		}

		public void setApplicationId(String applicationId) {
			this.applicationId = applicationId;
		}

		public Map<String, String> getConfiguration() {
			return configuration;
		}

		public void setConfiguration(Map<String, String> configuration) {
			this.configuration = configuration;
		}
	}

}
