/*
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.provisioning;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.common.ErrorMapping;
import kafka.utils.ZkUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.admin.AdminUtilsOperation;
import org.springframework.cloud.stream.binder.kafka.core.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.core.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.core.KafkaProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.Destination;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.ObjectUtils;

/**
 * @author Soby Chacko
 */
public class KafkaTopicProvisioner implements ProvisioningProvider<ExtendedConsumerProperties<KafkaConsumerProperties>,
		ExtendedProducerProperties<KafkaProducerProperties>>, InitializingBean {

	private final Log logger = LogFactory.getLog(getClass());

	private KafkaBinderConfigurationProperties configurationProperties;
	private final AdminUtilsOperation adminUtilsOperation;

	private RetryOperations metadataRetryOperations;

	public KafkaTopicProvisioner(KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties,
								AdminUtilsOperation adminUtilsOperation) {
		this.configurationProperties = kafkaBinderConfigurationProperties;
		this.adminUtilsOperation = adminUtilsOperation;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (this.metadataRetryOperations == null) {
			RetryTemplate retryTemplate = new RetryTemplate();

			SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
			simpleRetryPolicy.setMaxAttempts(10);
			retryTemplate.setRetryPolicy(simpleRetryPolicy);

			ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
			backOffPolicy.setInitialInterval(100);
			backOffPolicy.setMultiplier(2);
			backOffPolicy.setMaxInterval(1000);
			retryTemplate.setBackOffPolicy(backOffPolicy);
			this.metadataRetryOperations = retryTemplate;
		}
	}

	@Override
	public Destination provisionProducerDestination(final String name, ExtendedProducerProperties<KafkaProducerProperties> properties) {
		if (this.logger.isInfoEnabled()) {
			this.logger.info("Using kafka topic for outbound: " + name);
		}
		validateTopicName(name);
		createTopicsIfAutoCreateEnabledAndAdminUtilsPresent(name, properties.getPartitionCount());

		return new Destination() {
			@Override
			public String getDestination() {
				return name;
			}
		};
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(final String name, final String group, ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		validateTopicName(name);
		if (properties.getInstanceCount() == 0) {
			throw new IllegalArgumentException("Instance count cannot be zero");
		}
		int partitionCount = properties.getInstanceCount() * properties.getConcurrency();
		createTopicsIfAutoCreateEnabledAndAdminUtilsPresent(name, partitionCount);

		return new ConsumerDestination(){

			@Override
			public String getDestination() {
				return name;
			}

			@Override
			public String getPartitionedConsumerDestination(String group, int partition) {
				return name;
			}

			@Override
			public String getDlq() {
				return "error." + name + "." + group;
			}
		};
	}

	private void createTopicsIfAutoCreateEnabledAndAdminUtilsPresent(final String topicName, final int partitionCount) {
		if (this.configurationProperties.isAutoCreateTopics() && adminUtilsOperation != null) {
			createTopicAndPartitions(topicName, partitionCount);
		}
		else if (this.configurationProperties.isAutoCreateTopics() && adminUtilsOperation == null) {
			this.logger.warn("Auto creation of topics is enabled, but Kafka AdminUtils class is not present on the classpath. " +
					"No topic will be created by the binder");
		}
		else if (!this.configurationProperties.isAutoCreateTopics()) {
			this.logger.info("Auto creation of topics is disabled.");
		}
	}

	/**
	 * Creates a Kafka topic if needed, or try to increase its partition count to the
	 * desired number.
	 */
	private void createTopicAndPartitions(final String topicName, final int partitionCount) {

		final ZkUtils zkUtils = ZkUtils.apply(this.configurationProperties.getZkConnectionString(),
				this.configurationProperties.getZkSessionTimeout(),
				this.configurationProperties.getZkConnectionTimeout(),
				JaasUtils.isZkSecurityEnabled());
		try {
			short errorCode = adminUtilsOperation.errorCodeFromTopicMetadata(topicName, zkUtils);
			if (errorCode == ErrorMapping.NoError()) {
				// only consider minPartitionCount for resizing if autoAddPartitions is true
				int effectivePartitionCount = this.configurationProperties.isAutoAddPartitions()
						? Math.max(this.configurationProperties.getMinPartitionCount(), partitionCount)
						: partitionCount;
				int partitionSize = adminUtilsOperation.partitionSize(topicName, zkUtils);

				if (partitionSize < effectivePartitionCount) {
					if (this.configurationProperties.isAutoAddPartitions()) {
						adminUtilsOperation.invokeAddPartitions(zkUtils, topicName, effectivePartitionCount, null, false);
					}
					else {
						throw new BinderException("The number of expected partitions was: " + partitionCount + ", but "
								+ partitionSize + (partitionSize > 1 ? " have " : " has ") + "been found instead."
								+ "Consider either increasing the partition count of the topic or enabling " +
								"`autoAddPartitions`");
					}
				}
			}
			else if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
				// always consider minPartitionCount for topic creation
				final int effectivePartitionCount = Math.max(this.configurationProperties.getMinPartitionCount(),
						partitionCount);

				this.metadataRetryOperations.execute(new RetryCallback<Object, RuntimeException>() {

					@Override
					public Object doWithRetry(RetryContext context) throws RuntimeException {

						adminUtilsOperation.invokeCreateTopic(zkUtils, topicName, effectivePartitionCount,
								configurationProperties.getReplicationFactor(), new Properties());
						return null;
					}
				});
			}
			else {
				throw new BinderException("Error fetching Kafka topic metadata: ",
						ErrorMapping.exceptionFor(errorCode));
			}
		}
		finally {
			zkUtils.close();
		}
	}

	private DefaultKafkaProducerFactory<byte[], byte[]> getProducerFactory(
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {
		Map<String, Object> props = new HashMap<>();
		if (!ObjectUtils.isEmpty(configurationProperties.getConfiguration())) {
			props.putAll(configurationProperties.getConfiguration());
		}
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configurationProperties.getKafkaConnectionString());
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(producerProperties.getExtension().getBufferSize()));
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.ACKS_CONFIG, String.valueOf(this.configurationProperties.getRequiredAcks()));
		props.put(ProducerConfig.LINGER_MS_CONFIG,
				String.valueOf(producerProperties.getExtension().getBatchTimeout()));
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
				producerProperties.getExtension().getCompressionType().toString());
		if (!ObjectUtils.isEmpty(producerProperties.getExtension().getConfiguration())) {
			props.putAll(producerProperties.getExtension().getConfiguration());
		}
		return new DefaultKafkaProducerFactory<>(props);
	}

	/**
	 * Retry configuration for operations such as validating topic creation
	 *
	 * @param metadataRetryOperations the retry configuration
	 */
	public void setMetadataRetryOperations(RetryOperations metadataRetryOperations) {
		this.metadataRetryOperations = metadataRetryOperations;
	}

	/**
	 * Allowed chars are ASCII alphanumerics, '.', '_' and '-'.
	 */
	private void validateTopicName(String topicName) {
		try {
			byte[] utf8 = topicName.getBytes("UTF-8");
			for (byte b : utf8) {
				if (!((b >= 'a') && (b <= 'z') || (b >= 'A') && (b <= 'Z') || (b >= '0') && (b <= '9') || (b == '.')
						|| (b == '-') || (b == '_'))) {
					throw new IllegalArgumentException(
							"Topic name can only have ASCII alphanumerics, '.', '_' and '-'");
				}
			}
		}
		catch (UnsupportedEncodingException e) {
			throw new AssertionError(e); // Can't happen
		}
	}

}
