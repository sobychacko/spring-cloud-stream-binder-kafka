/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kstream;

import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;

import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamConsumerProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamProducerProperties;
import org.springframework.util.StringUtils;

/**
 * @author Soby Chacko
 */
public class KeyValueSerdeResolver {

	private final Map<String,Object> streamConfigGlobalProperties;

	private final KStreamBinderConfigurationProperties binderConfigurationProperties;

	public KeyValueSerdeResolver(Map<String,Object> streamConfigGlobalProperties,
								 KStreamBinderConfigurationProperties binderConfigurationProperties) {
		this.streamConfigGlobalProperties = streamConfigGlobalProperties;
		this.binderConfigurationProperties = binderConfigurationProperties;
	}

	public Serde<?> getInboundKeySerde(KStreamConsumerProperties extendedConsumerProperties) {
		String keySerdeString = extendedConsumerProperties.getKeySerde();

		return getKeySerde(keySerdeString);
	}

	public Serde<?> getInboundValueSerde(ConsumerProperties consumerProperties, KStreamConsumerProperties extendedConsumerProperties) {
		Serde<?> valueSerde;

		String valueSerdeString = extendedConsumerProperties.getValueSerde();
		try {
			if (consumerProperties != null &&
					consumerProperties.isUseNativeDecoding()) {
				valueSerde = getValueSerde(valueSerdeString);
			}
			else {
				valueSerde = Serdes.ByteArray();
			}
			valueSerde.configure(streamConfigGlobalProperties, false);
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException("Serde class not found: ", e);
		}
		return valueSerde;
	}

	public Serde<?> getOuboundKeySerde(KStreamProducerProperties properties) {
		return getKeySerde(properties.getKeySerde());
	}

	public Serde<?> getOutboundValueSerde(ProducerProperties producerProperties, KStreamProducerProperties kStreamProducerProperties) {
		Serde<?> valueSerde;
		try {
			if (producerProperties.isUseNativeEncoding()) {
				valueSerde = getValueSerde(kStreamProducerProperties.getValueSerde());
			}
			else {
				valueSerde = Serdes.ByteArray();
			}
			valueSerde.configure(streamConfigGlobalProperties, false);
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException("Serde class not found: ", e);
		}
		return valueSerde;
	}

	private Serde<?> getKeySerde(String keySerdeString) {
		Serde<?> keySerde;
		try {
			if (StringUtils.hasText(keySerdeString)) {
				keySerde = Utils.newInstance(keySerdeString, Serde.class);
			}
			else {
				keySerde = this.binderConfigurationProperties.getConfiguration().containsKey("key.serde") ?
						Utils.newInstance(this.binderConfigurationProperties.getConfiguration().get("key.serde"), Serde.class) : Serdes.ByteArray();
			}
			keySerde.configure(streamConfigGlobalProperties, true);

		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Serde class not found: ", e);
		}
		return keySerde;
	}

	private Serde<?> getValueSerde(String valueSerdeString) throws ClassNotFoundException {
		Serde<?> valueSerde;
		if (StringUtils.hasText(valueSerdeString)) {
			valueSerde = Utils.newInstance(valueSerdeString, Serde.class);
		}
		else {
			valueSerde = this.binderConfigurationProperties.getConfiguration().containsKey("value.serde") ?
					Utils.newInstance(this.binderConfigurationProperties.getConfiguration().get("value.serde"), Serde.class) : Serdes.ByteArray();
		}
		return valueSerde;
	}
}
