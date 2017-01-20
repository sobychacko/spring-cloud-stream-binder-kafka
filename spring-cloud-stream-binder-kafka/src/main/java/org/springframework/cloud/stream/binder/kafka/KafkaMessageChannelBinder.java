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

package org.springframework.cloud.stream.binder.kafka;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;

import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.core.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.core.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.core.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.core.KafkaProducerProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.Lifecycle;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * A {@link Binder} that uses Kafka as the underlying middleware.
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Gary Russell
 * @author Mark Fisher
 * @author Soby Chacko
 */
public class KafkaMessageChannelBinder extends
		AbstractMessageChannelBinder<ExtendedConsumerProperties<KafkaConsumerProperties>,
				ExtendedProducerProperties<KafkaProducerProperties>, Collection<PartitionInfo>, String, Producer<byte[], byte[]>>
		implements ExtendedPropertiesBinder<MessageChannel, KafkaConsumerProperties, KafkaProducerProperties> {

	private final KafkaBinderConfigurationProperties configurationProperties;

	private ProducerListener<byte[], byte[]> producerListener;

	private KafkaExtendedBindingProperties extendedBindingProperties = new KafkaExtendedBindingProperties();

	ProvisioningProvider<ExtendedConsumerProperties<KafkaConsumerProperties>,
			ExtendedProducerProperties<KafkaProducerProperties>,
			Collection<PartitionInfo>, String, Producer<byte[], byte[]>> provisioningProvider;

	public KafkaMessageChannelBinder(KafkaBinderConfigurationProperties configurationProperties,
									ProvisioningProvider<ExtendedConsumerProperties<KafkaConsumerProperties>,
									ExtendedProducerProperties<KafkaProducerProperties>,
											Collection<PartitionInfo>, String, Producer<byte[], byte[]>> provisioningProvider) {
		super(false, headersToMap(configurationProperties), provisioningProvider);
		this.configurationProperties = configurationProperties;
		this.provisioningProvider = provisioningProvider;
	}

	private static String[] headersToMap(KafkaBinderConfigurationProperties configurationProperties) {
		String[] headersToMap;
		if (ObjectUtils.isEmpty(configurationProperties.getHeaders())) {
			headersToMap = BinderHeaders.STANDARD_HEADERS;
		}
		else {
			String[] combinedHeadersToMap = Arrays.copyOfRange(BinderHeaders.STANDARD_HEADERS, 0,
					BinderHeaders.STANDARD_HEADERS.length + configurationProperties.getHeaders().length);
			System.arraycopy(configurationProperties.getHeaders(), 0, combinedHeadersToMap,
					BinderHeaders.STANDARD_HEADERS.length,
					configurationProperties.getHeaders().length);
			headersToMap = combinedHeadersToMap;
		}
		return headersToMap;
	}

	public void setExtendedBindingProperties(KafkaExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}

	public void setProducerListener(ProducerListener<byte[], byte[]> producerListener) {
		this.producerListener = producerListener;
	}

	@Override
	public KafkaConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KafkaProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	@Override
	protected MessageHandler createProducerMessageHandler(final String destination,
															ExtendedProducerProperties<KafkaProducerProperties> producerProperties) throws Exception {

		DefaultKafkaProducerFactory<byte[], byte[]> producerFB = getProducerFactory(producerProperties);
		KafkaTemplate<byte[], byte[]> kafkaTemplate = new KafkaTemplate<>(producerFB);
		if (this.producerListener != null) {
			kafkaTemplate.setProducerListener(this.producerListener);
		}
		return new ProducerConfigurationMessageHandler(kafkaTemplate, destination, producerProperties, producerFB);
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

	@Override
	@SuppressWarnings("unchecked")
	protected MessageProducer createConsumerEndpoint(String name, String group, Collection<PartitionInfo> destination,
													ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		boolean anonymous = !StringUtils.hasText(group);
		Assert.isTrue(!anonymous || !properties.getExtension().isEnableDlq(),
				"DLQ support is not available for anonymous subscriptions");
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString() : group;
		Map<String, Object> props = getConsumerConfig(anonymous, consumerGroup);
		if (!ObjectUtils.isEmpty(properties.getExtension().getConfiguration())) {
			props.putAll(properties.getExtension().getConfiguration());
		}
		ConsumerFactory<?, ?> consumerFactory = new DefaultKafkaConsumerFactory<>(props);
		Collection<PartitionInfo> listenedPartitions = destination;
		Assert.isTrue(!CollectionUtils.isEmpty(listenedPartitions), "A list of partitions must be provided");
		final TopicPartitionInitialOffset[] topicPartitionInitialOffsets = getTopicPartitionInitialOffsets(
				listenedPartitions);
		final ContainerProperties containerProperties =
				anonymous || properties.getExtension().isAutoRebalanceEnabled() ? new ContainerProperties(name)
						: new ContainerProperties(topicPartitionInitialOffsets);
		int concurrency = Math.min(properties.getConcurrency(), listenedPartitions.size());
		final ConcurrentMessageListenerContainer<?, ?> messageListenerContainer =
				new ConcurrentMessageListenerContainer(
						consumerFactory, containerProperties) {

					@Override
					public void stop(Runnable callback) {
						super.stop(callback);
					}
				};
		messageListenerContainer.setConcurrency(concurrency);
		messageListenerContainer.getContainerProperties().setAckOnError(isAutoCommitOnError(properties));
		if (!properties.getExtension().isAutoCommitOffset()) {
			messageListenerContainer.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);
		}
		if (this.logger.isDebugEnabled()) {
			this.logger.debug(
					"Listened partitions: " + StringUtils.collectionToCommaDelimitedString(listenedPartitions));
		}
		if (this.logger.isDebugEnabled()) {
			this.logger.debug(
					"Listened partitions: " + StringUtils.collectionToCommaDelimitedString(listenedPartitions));
		}
		final KafkaMessageDrivenChannelAdapter<?, ?> kafkaMessageDrivenChannelAdapter =
				new KafkaMessageDrivenChannelAdapter<>(
						messageListenerContainer);
		kafkaMessageDrivenChannelAdapter.setBeanFactory(this.getBeanFactory());
		final RetryTemplate retryTemplate = buildRetryTemplate(properties);
		kafkaMessageDrivenChannelAdapter.setRetryTemplate(retryTemplate);
		if (properties.getExtension().isEnableDlq()) {
			final String dlqTopic = "error." + name + "." + group;
			final Producer<byte[], byte[]> dlq = provisioningProvider.provisionDlq();
			messageListenerContainer.getContainerProperties().setErrorHandler(new ErrorHandler() {

				@Override
				public void handle(Exception thrownException, final ConsumerRecord message) {
					final byte[] key = message.key() != null ? Utils.toArray(ByteBuffer.wrap((byte[]) message.key()))
							: null;
					final byte[] payload = message.value() != null
							? Utils.toArray(ByteBuffer.wrap((byte[]) message.value())) : null;
					dlq.send(new ProducerRecord<>(dlqTopic, key, payload),
							new Callback() {

								@Override
								public void onCompletion(RecordMetadata metadata, Exception exception) {
									StringBuffer messageLog = new StringBuffer();
									messageLog.append(" a message with key='"
											+ toDisplayString(ObjectUtils.nullSafeToString(key), 50) + "'");
									messageLog.append(" and payload='"
											+ toDisplayString(ObjectUtils.nullSafeToString(payload), 50) + "'");
									messageLog.append(" received from " + message.partition());
									if (exception != null) {
										KafkaMessageChannelBinder.this.logger.error(
												"Error sending to DLQ" + messageLog.toString(), exception);
									}
									else {
										if (KafkaMessageChannelBinder.this.logger.isDebugEnabled()) {
											KafkaMessageChannelBinder.this.logger.debug(
													"Sent to DLQ " + messageLog.toString());
										}
									}
								}
							});
				}
			});
		}
		return kafkaMessageDrivenChannelAdapter;
	}

	private Map<String, Object> getConsumerConfig(boolean anonymous, String consumerGroup) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		if (!ObjectUtils.isEmpty(configurationProperties.getConfiguration())) {
			props.putAll(configurationProperties.getConfiguration());
		}
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configurationProperties.getKafkaConnectionString());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				anonymous ? "latest" : "earliest");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
		return props;
	}

	private boolean isAutoCommitOnError(ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		return properties.getExtension().getAutoCommitOnError() != null
				? properties.getExtension().getAutoCommitOnError()
				: properties.getExtension().isAutoCommitOffset() && properties.getExtension().isEnableDlq();
	}

	private TopicPartitionInitialOffset[] getTopicPartitionInitialOffsets(
			Collection<PartitionInfo> listenedPartitions) {
		final TopicPartitionInitialOffset[] topicPartitionInitialOffsets =
				new TopicPartitionInitialOffset[listenedPartitions.size()];
		int i = 0;
		for (PartitionInfo partition : listenedPartitions) {

			topicPartitionInitialOffsets[i++] = new TopicPartitionInitialOffset(partition.topic(),
					partition.partition());
		}
		return topicPartitionInitialOffsets;
	}

	private String toDisplayString(String original, int maxCharacters) {
		if (original.length() <= maxCharacters) {
			return original;
		}
		return original.substring(0, maxCharacters) + "...";
	}

	private final class ProducerConfigurationMessageHandler extends KafkaProducerMessageHandler<byte[], byte[]>
			implements Lifecycle {

		private boolean running = true;

		private final DefaultKafkaProducerFactory<byte[], byte[]> producerFactory;

		private ProducerConfigurationMessageHandler(KafkaTemplate<byte[], byte[]> kafkaTemplate, String topic,
													ExtendedProducerProperties<KafkaProducerProperties> producerProperties,
													DefaultKafkaProducerFactory<byte[], byte[]> producerFactory) {
			super(kafkaTemplate);
			setTopicExpression(new LiteralExpression(topic));
			setBeanFactory(KafkaMessageChannelBinder.this.getBeanFactory());
			if (producerProperties.isPartitioned()) {
				SpelExpressionParser parser = new SpelExpressionParser();
				setPartitionIdExpression(parser.parseExpression("headers.partition"));
			}
			if (producerProperties.getExtension().isSync()) {
				setSync(true);
			}
			this.producerFactory = producerFactory;
		}

		@Override
		public void start() {
			try {
				super.onInit();
			}
			catch (Exception e) {
				this.logger.error("Initialization errors: ", e);
				throw new RuntimeException(e);
			}
		}

		@Override
		public void stop() {
			producerFactory.stop();
			this.running = false;
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}
	}
}
