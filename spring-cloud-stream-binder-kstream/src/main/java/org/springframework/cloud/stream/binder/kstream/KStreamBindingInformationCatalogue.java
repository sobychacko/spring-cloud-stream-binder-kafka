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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.streams.kstream.KStream;

import org.springframework.cloud.stream.binder.kstream.config.KStreamConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;

/**
 * A catalogue containing all the inbound and outboud KStreams.
 * It registers {@link BindingProperties} and {@link KStreamConsumerProperties}
 * for the bounded KStreams. This registry provides services for finding
 * specific binding level information for the bounded KStream. This includes
 * information such as the configured content type, destination etc.
 *
 * @author Soby Chacko
 */
public class KStreamBindingInformationCatalogue {

	private Map<KStream<?, ?>, BindingProperties> bindingProperties = new ConcurrentHashMap<>();
	private Map<KStream<?, ?>, KStreamConsumerProperties> consumerProperties = new ConcurrentHashMap<>();

	public String getDestination(KStream<?,?> bindingTarget) {
		BindingProperties bindingProperties = this.bindingProperties.get(bindingTarget);
		return bindingProperties.getDestination();
	}

	public boolean isUseNativeDecoding(KStream<?,?> bindingTarget) {
		BindingProperties bindingProperties = this.bindingProperties.get(bindingTarget);
		return bindingProperties.getConsumer().isUseNativeDecoding();
	}

	public boolean isEnableDlq(KStream<?,?> bindingTarget) {
		return consumerProperties.get(bindingTarget).isEnableDlq();
	}

	public String getContentType(KStream<?,?> bindingTarget) {
		BindingProperties bindingProperties = this.bindingProperties.get(bindingTarget);
		return bindingProperties.getContentType();
	}

	public KStreamConsumerProperties.SerdeError getSerdeError(KStream<?,?> bindingTarget) {
		return consumerProperties.get(bindingTarget).getSerdeError();
	}

	public void registerBindingProperties(KStream<?,?> bindingTarget, BindingProperties bindingProperties) {
		this.bindingProperties.put(bindingTarget, bindingProperties);
	}

	public void registerConsumerProperties(KStream<?,?> bindingTarget, KStreamConsumerProperties kStreamConsumerProperties) {
		this.consumerProperties.put(bindingTarget, kStreamConsumerProperties);
	}

}
