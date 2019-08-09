/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.StringUtils;

/**
 * @author Soby Chacko
 * @since 3.0.0
 */
public abstract class AbstractKafkaStreamsBinderProcessor implements ApplicationContextAware {

	private static final Log LOG = LogFactory.getLog(AbstractKafkaStreamsBinderProcessor.class);

	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;
	private final BindingServiceProperties bindingServiceProperties;
	private final KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties;
	private final CleanupConfig cleanupConfig;
	private final KeyValueSerdeResolver keyValueSerdeResolver;

	protected ConfigurableApplicationContext applicationContext;

	public AbstractKafkaStreamsBinderProcessor(BindingServiceProperties bindingServiceProperties,
											KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue,
											KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties,
											KeyValueSerdeResolver keyValueSerdeResolver, CleanupConfig cleanupConfig) {
		this.bindingServiceProperties = bindingServiceProperties;
		this.kafkaStreamsBindingInformationCatalogue = kafkaStreamsBindingInformationCatalogue;
		this.kafkaStreamsExtendedBindingProperties = kafkaStreamsExtendedBindingProperties;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
		this.cleanupConfig = cleanupConfig;
	}

	private <K, V> KTable<K, V> materializedAs(StreamsBuilder streamsBuilder,
											String destination, String storeName, Serde<K> k, Serde<V> v,
											Topology.AutoOffsetReset autoOffsetReset) {
		return streamsBuilder.table(
				this.bindingServiceProperties.getBindingDestination(destination),
				Consumed.with(k, v).withOffsetResetPolicy(autoOffsetReset),
				getMaterialized(storeName, k, v));
	}

	protected <K, V> GlobalKTable<K, V> materializedAsGlobalKTable(
			StreamsBuilder streamsBuilder, String destination, String storeName,
			Serde<K> k, Serde<V> v, Topology.AutoOffsetReset autoOffsetReset) {
		return streamsBuilder.globalTable(
				this.bindingServiceProperties.getBindingDestination(destination),
				Consumed.with(k, v).withOffsetResetPolicy(autoOffsetReset),
				getMaterialized(storeName, k, v));
	}

	protected GlobalKTable<?, ?> getGlobalKTable(StreamsBuilder streamsBuilder,
												Serde<?> keySerde, Serde<?> valueSerde, String materializedAs,
												String bindingDestination, Topology.AutoOffsetReset autoOffsetReset) {
		return materializedAs != null
				? materializedAsGlobalKTable(streamsBuilder, bindingDestination,
				materializedAs, keySerde, valueSerde, autoOffsetReset)
				: streamsBuilder.globalTable(bindingDestination,
				Consumed.with(keySerde, valueSerde)
						.withOffsetResetPolicy(autoOffsetReset));
	}

	protected KTable<?, ?> getKTable(StreamsBuilder streamsBuilder, Serde<?> keySerde,
									Serde<?> valueSerde, String materializedAs, String bindingDestination,
									Topology.AutoOffsetReset autoOffsetReset) {
		return materializedAs != null
				? materializedAs(streamsBuilder, bindingDestination, materializedAs,
				keySerde, valueSerde, autoOffsetReset)
				: streamsBuilder.table(bindingDestination,
				Consumed.with(keySerde, valueSerde)
						.withOffsetResetPolicy(autoOffsetReset));
	}

	private <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> getMaterialized(
			String storeName, Serde<K> k, Serde<V> v) {
		return Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName)
				.withKeySerde(k).withValueSerde(v);
	}

	protected Topology.AutoOffsetReset getAutoOffsetReset(String inboundName, KafkaStreamsConsumerProperties extendedConsumerProperties) {
		final KafkaConsumerProperties.StartOffset startOffset = extendedConsumerProperties
				.getStartOffset();
		Topology.AutoOffsetReset autoOffsetReset = null;
		if (startOffset != null) {
			switch (startOffset) {
				case earliest:
					autoOffsetReset = Topology.AutoOffsetReset.EARLIEST;
					break;
				case latest:
					autoOffsetReset = Topology.AutoOffsetReset.LATEST;
					break;
				default:
					break;
			}
		}
		if (extendedConsumerProperties.isResetOffsets()) {
			AbstractKafkaStreamsBinderProcessor.LOG.warn("Detected resetOffsets configured on binding "
					+ inboundName + ". "
					+ "Setting resetOffsets in Kafka Streams binder does not have any effect.");
		}
		return autoOffsetReset;
	}

	@SuppressWarnings("unchecked")
	protected void handleKTableGlobalKTableInputs(Object[] arguments, int index, String input, Class<?> parameterType, Object targetBean,
												StreamsBuilderFactoryBean streamsBuilderFactoryBean, StreamsBuilder streamsBuilder,
												KafkaStreamsConsumerProperties extendedConsumerProperties,
												Serde<?> keySerde, Serde<?> valueSerde, Topology.AutoOffsetReset autoOffsetReset) {
		if (parameterType.isAssignableFrom(KTable.class)) {
			String materializedAs = extendedConsumerProperties.getMaterializedAs();
			String bindingDestination = this.bindingServiceProperties.getBindingDestination(input);
			KTable<?, ?> table = getKTable(streamsBuilder, keySerde, valueSerde, materializedAs,
					bindingDestination, autoOffsetReset);
			KTableBoundElementFactory.KTableWrapper kTableWrapper =
					(KTableBoundElementFactory.KTableWrapper) targetBean;
			//wrap the proxy created during the initial target type binding with real object (KTable)
			kTableWrapper.wrap((KTable<Object, Object>) table);
			this.kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactory(streamsBuilderFactoryBean);
			arguments[index] = table;
		}
		else if (parameterType.isAssignableFrom(GlobalKTable.class)) {
			String materializedAs = extendedConsumerProperties.getMaterializedAs();
			String bindingDestination = this.bindingServiceProperties.getBindingDestination(input);
			GlobalKTable<?, ?> table = getGlobalKTable(streamsBuilder, keySerde, valueSerde, materializedAs,
					bindingDestination, autoOffsetReset);
			GlobalKTableBoundElementFactory.GlobalKTableWrapper globalKTableWrapper =
					(GlobalKTableBoundElementFactory.GlobalKTableWrapper) targetBean;
			//wrap the proxy created during the initial target type binding with real object (KTable)
			globalKTableWrapper.wrap((GlobalKTable<Object, Object>) table);
			this.kafkaStreamsBindingInformationCatalogue.addStreamBuilderFactory(streamsBuilderFactoryBean);
			arguments[index] = table;
		}
	}

	@SuppressWarnings({"unchecked"})
	protected StreamsBuilderFactoryBean buildStreamsBuilderAndRetrieveConfig(String beanNamePostPrefix,
																			ApplicationContext applicationContext, String inboundName) {
		ConfigurableListableBeanFactory beanFactory = this.applicationContext
				.getBeanFactory();

		Map<String, Object> streamConfigGlobalProperties = applicationContext
				.getBean("streamConfigGlobalProperties", Map.class);

		KafkaStreamsConsumerProperties extendedConsumerProperties = this.kafkaStreamsExtendedBindingProperties
				.getExtendedConsumerProperties(inboundName);
		streamConfigGlobalProperties
				.putAll(extendedConsumerProperties.getConfiguration());

		String bindingLevelApplicationId = extendedConsumerProperties.getApplicationId();
		// override application.id if set at the individual binding level.
		if (StringUtils.hasText(bindingLevelApplicationId)) {
			streamConfigGlobalProperties.put(StreamsConfig.APPLICATION_ID_CONFIG,
					bindingLevelApplicationId);
		}

		//If the application id is not set by any mechanism, then generate it.
		streamConfigGlobalProperties.computeIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG,
				k -> {
					String generatedApplicationID = beanNamePostPrefix + "-" + UUID.randomUUID().toString() + "-applicationId";
					LOG.info("Generated Kafka Streams Application ID: " + generatedApplicationID);
					return generatedApplicationID;
				});

		int concurrency = this.bindingServiceProperties.getConsumerProperties(inboundName)
				.getConcurrency();
		// override concurrency if set at the individual binding level.
		if (concurrency > 1) {
			streamConfigGlobalProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
					concurrency);
		}

		Map<String, KafkaStreamsDlqDispatch> kafkaStreamsDlqDispatchers = applicationContext
				.getBean("kafkaStreamsDlqDispatchers", Map.class);

		KafkaStreamsConfiguration kafkaStreamsConfiguration = new KafkaStreamsConfiguration(
				streamConfigGlobalProperties) {
			@Override
			public Properties asProperties() {
				Properties properties = super.asProperties();
				properties.put(SendToDlqAndContinue.KAFKA_STREAMS_DLQ_DISPATCHERS,
						kafkaStreamsDlqDispatchers);
				return properties;
			}
		};

		StreamsBuilderFactoryBean streamsBuilder = this.cleanupConfig == null
				? new StreamsBuilderFactoryBean(kafkaStreamsConfiguration)
				: new StreamsBuilderFactoryBean(kafkaStreamsConfiguration,
				this.cleanupConfig);
		streamsBuilder.setAutoStartup(false);
		BeanDefinition streamsBuilderBeanDefinition = BeanDefinitionBuilder
				.genericBeanDefinition(
						(Class<StreamsBuilderFactoryBean>) streamsBuilder.getClass(),
						() -> streamsBuilder)
				.getRawBeanDefinition();
		((BeanDefinitionRegistry) beanFactory).registerBeanDefinition(
				"stream-builder-" + beanNamePostPrefix, streamsBuilderBeanDefinition);

		return applicationContext.getBean(
				"&stream-builder-" + beanNamePostPrefix, StreamsBuilderFactoryBean.class);
	}

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	protected KStream<?, ?> getkStream(BindingProperties bindingProperties, KStream<?, ?> stream, boolean nativeDecoding) {
		stream = stream.mapValues((value) -> {
			Object returnValue;
			String contentType = bindingProperties.getContentType();
			if (value != null && !StringUtils.isEmpty(contentType) && !nativeDecoding) {
				returnValue = MessageBuilder.withPayload(value)
						.setHeader(MessageHeaders.CONTENT_TYPE, contentType).build();
			}
			else {
				returnValue = value;
			}
			return returnValue;
		});
		return stream;
	}

	protected Serde<?> getValueSerde(String inboundName, KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties, ResolvableType resolvableType) {
		if (bindingServiceProperties.getConsumerProperties(inboundName).isUseNativeDecoding()) {
			BindingProperties bindingProperties = this.bindingServiceProperties
					.getBindingProperties(inboundName);
			return this.keyValueSerdeResolver.getInboundValueSerde(
					bindingProperties.getConsumer(), kafkaStreamsConsumerProperties, resolvableType);
		}
		else {
			return Serdes.ByteArray();
		}
	}
}
