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

import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kstream.config.KStreamConsumerProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamExtendedBindingProperties;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.cloud.stream.binding.StreamListenerSetupMethodOrchestrator;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.core.StreamsBuilderFactoryBean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Kafka Streams specific implementation for {@link StreamListenerSetupMethodOrchestrator}
 * that overrides the default mechanisms for invoking StreamListener adapters.
 *
 * @author Soby Chacko
 */
public class KStreamListenerSetupMethodOrchestrator implements StreamListenerSetupMethodOrchestrator, ApplicationContextAware {

	private ConfigurableApplicationContext applicationContext;

	private StreamListenerParameterAdapter streamListenerParameterAdapter;
	private Collection<StreamListenerResultAdapter> streamListenerResultAdapters;

	private BindingServiceProperties bindingServiceProperties;
	private KStreamExtendedBindingProperties kStreamExtendedBindingProperties;
	private KeyValueSerdeResolver keyValueSerdeResolver;
	private KStreamBindingInformationCatalogue kStreamBindingInformationCatalogue;

	public KStreamListenerSetupMethodOrchestrator(BindingServiceProperties bindingServiceProperties,
												  KStreamExtendedBindingProperties kStreamExtendedBindingProperties,
												  KeyValueSerdeResolver keyValueSerdeResolver,
												  KStreamBindingInformationCatalogue kStreamBindingInformationCatalogue,
												  StreamListenerParameterAdapter streamListenerParameterAdapter,
												  Collection<StreamListenerResultAdapter> streamListenerResultAdapters) {
		this.bindingServiceProperties = bindingServiceProperties;
		this.kStreamExtendedBindingProperties = kStreamExtendedBindingProperties;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
		this.kStreamBindingInformationCatalogue = kStreamBindingInformationCatalogue;
		this.streamListenerParameterAdapter = streamListenerParameterAdapter;
		this.streamListenerResultAdapters = streamListenerResultAdapters;
	}

	@Override
	public boolean supports(Method method) {
		return methodParameterSuppports(method) && methodReturnTypeSuppports(method);
	}

	private boolean methodReturnTypeSuppports(Method method) {
		Class<?> returnType = method.getReturnType();
		if (returnType.equals(KStream.class) ||
				(returnType.isArray() && returnType.getComponentType().equals(KStream.class))) {
			return true;
		}
		return false;
	}

	private boolean methodParameterSuppports(Method method) {
		MethodParameter methodParameter = MethodParameter.forExecutable(method, 0);
		Class<?> parameterType = methodParameter.getParameterType();
		return parameterType.equals(KStream.class);
	}

	@Override
	@SuppressWarnings({"unchecked"})
	public Object[] adaptAndRetrieveInboundArguments(Method method, String inboundName,
													 ApplicationContext applicationContext,
													 StreamListenerParameterAdapter... streamListenerParameterAdapters) {

		Object[] arguments = new Object[method.getParameterTypes().length];
		for (int parameterIndex = 0; parameterIndex < arguments.length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forExecutable(method, parameterIndex);
			Class<?> parameterType = methodParameter.getParameterType();
			Object targetReferenceValue = null;
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				targetReferenceValue = AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Input.class));
			}
			else if (arguments.length == 1 && StringUtils.hasText(inboundName)) {
				targetReferenceValue = inboundName;
			}
			if (targetReferenceValue != null) {
				Assert.isInstanceOf(String.class, targetReferenceValue, "Annotation value must be a String");
				Object targetBean = applicationContext.getBean((String) targetReferenceValue);

				KStreamBoundElementFactory.KStreamWrapper kStreamWrapper = (KStreamBoundElementFactory.KStreamWrapper)targetBean;


				BindingProperties bindingProperties = bindingServiceProperties.getBindingProperties(inboundName);
				String destination = bindingProperties.getDestination();
				if (destination == null) {
					destination = inboundName;
				}
				KStreamConsumerProperties extendedConsumerProperties = kStreamExtendedBindingProperties.getExtendedConsumerProperties(inboundName);
				Serde<?> keySerde = this.keyValueSerdeResolver.getInboundKeySerde(extendedConsumerProperties);

				Serde<?> valueSerde = this.keyValueSerdeResolver.getInboundValueSerde(bindingProperties.getConsumer(),
						extendedConsumerProperties);

				ConfigurableListableBeanFactory beanFactory = this.applicationContext.getBeanFactory();
				StreamsBuilderFactoryBean streamsBuilder = new StreamsBuilderFactoryBean();
				streamsBuilder.setAutoStartup(false);
				beanFactory.registerSingleton("stream-builder-" + destination, streamsBuilder);
				beanFactory.initializeBean(streamsBuilder, "stream-builder-" + destination);

				StreamsBuilder streamBuilder = null;
				try {
					streamBuilder = streamsBuilder.getObject();
				} catch (Exception e) {
					//log and bail
				}

				KStream<?,?> stream = streamBuilder.stream(bindingServiceProperties.getBindingDestination(inboundName),
								Consumed.with(keySerde, valueSerde));
				stream = stream.map((key, value) -> {
					KeyValue<Object, Object> keyValue;
					String contentType = bindingProperties.getContentType();
					if (!StringUtils.isEmpty(contentType) && !bindingProperties.getConsumer().isUseNativeDecoding()) {
						Message<?> message = MessageBuilder.withPayload(value)
								.setHeader(MessageHeaders.CONTENT_TYPE, contentType).build();
						keyValue = new KeyValue<>(key, message);
					}
					else {
						keyValue = new KeyValue<>(key, value);
					}
					return keyValue;
				});
				kStreamWrapper.wrap((KStream<Object, Object>) stream);

				//this.kStreamBindingInformationCatalogue.registerBindingProperties(kStreamWrapper, bindingProperties);

				// Iterate existing parameter adapters first
				for (StreamListenerParameterAdapter streamListenerParameterAdapter : streamListenerParameterAdapters) {
					if (streamListenerParameterAdapter.supports(stream.getClass(), methodParameter)) {
						arguments[parameterIndex] = streamListenerParameterAdapter.adapt(kStreamWrapper, methodParameter);
						break;
					}
				}
				if (arguments[parameterIndex] == null && parameterType.isAssignableFrom(stream.getClass())) {
					arguments[parameterIndex] = stream;
				}
				Assert.notNull(arguments[parameterIndex], "Cannot convert argument " + parameterIndex + " of " + method
						+ "from " + stream.getClass() + " to " + parameterType);
			}
			else {
				throw new IllegalStateException(StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
			}
		}
		return arguments;
	}

	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void orchestrateStreamListenerSetupMethod(StreamListener streamListener, Method method, Object bean) {
		String[] methodAnnotatedOutboundNames = getOutboundBindingTargetNames(method);
		validateStreamListenerMethod(streamListener, method, methodAnnotatedOutboundNames);

		String methodAnnotatedInboundName = streamListener.value();
		Object[] adaptedInboundArguments = adaptAndRetrieveInboundArguments(method, methodAnnotatedInboundName,
				this.applicationContext,
				this.streamListenerParameterAdapter);

		try {
			Object result = method.invoke(bean, adaptedInboundArguments);

			if (result.getClass().isArray()) {
				Assert.isTrue(methodAnnotatedOutboundNames.length == ((Object[]) result).length, "Big error");
			} else {
				Assert.isTrue(methodAnnotatedOutboundNames.length == 1, "Big error");
			}
			if (result.getClass().isArray()) {
				Object[] outboundKStreams = (Object[]) result;
				int i = 0;
				for (Object outboundKStream : outboundKStreams) {
					Object targetBean = this.applicationContext.getBean(methodAnnotatedOutboundNames[i++]);
					for (StreamListenerResultAdapter streamListenerResultAdapter : streamListenerResultAdapters) {
						if (streamListenerResultAdapter.supports(outboundKStream.getClass(), targetBean.getClass())) {
							streamListenerResultAdapter.adapt(outboundKStream, targetBean);
							break;
						}
					}
				}
			}
			else {
				Object targetBean = this.applicationContext.getBean(methodAnnotatedOutboundNames[0]);
				for (StreamListenerResultAdapter streamListenerResultAdapter : streamListenerResultAdapters) {
					if (streamListenerResultAdapter.supports(result.getClass(), targetBean.getClass())) {
						streamListenerResultAdapter.adapt(result, targetBean);
						break;
					}
				}
			}
		}
		catch (Exception e) {
			throw new BeanInitializationException("Cannot setup StreamListener for " + method, e);
		}
	}

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	private void validateStreamListenerMethod(StreamListener streamListener, Method method, String[] methodAnnotatedOutboundNames) {
		String methodAnnotatedInboundName = streamListener.value();
		for (String s : methodAnnotatedOutboundNames) {
			if (StringUtils.hasText(s)) {
				Assert.isTrue(isDeclarativeOutput(method, s), "Method must be declarative");
			}
		}
		if (StringUtils.hasText(methodAnnotatedInboundName)) {
			int methodArgumentsLength = method.getParameterTypes().length;

			for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
				MethodParameter methodParameter = MethodParameter.forExecutable(method, parameterIndex);
				Assert.isTrue(isDeclarativeInput(methodAnnotatedInboundName, methodParameter), "Method must be declarative");
			}
		}
	}

	@SuppressWarnings("unchecked")
	private boolean isDeclarativeOutput(Method m, String targetBeanName) {
		boolean declarative;
		Class<?> returnType = m.getReturnType();
		if (returnType.isArray()){
			Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
			declarative = this.streamListenerResultAdapters.stream()
					.anyMatch(slpa -> slpa.supports(returnType.getComponentType(), targetBeanClass));
			return declarative;
		}
		Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
		declarative = this.streamListenerResultAdapters.stream()
				.anyMatch(slpa -> slpa.supports(returnType, targetBeanClass));
		return declarative;
	}

	@SuppressWarnings("unchecked")
	private boolean isDeclarativeInput(String targetBeanName, MethodParameter methodParameter) {
		if (!methodParameter.getParameterType().isAssignableFrom(Object.class) && this.applicationContext.containsBean(targetBeanName)) {
			Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
			return this.streamListenerParameterAdapter.supports(targetBeanClass, methodParameter);
		}
		return false;
	}

	private static String[] getOutboundBindingTargetNames(Method method) {
		SendTo sendTo = AnnotationUtils.findAnnotation(method, SendTo.class);
		if (sendTo != null) {
			Assert.isTrue(!ObjectUtils.isEmpty(sendTo.value()), StreamListenerErrorMessages.ATLEAST_ONE_OUTPUT);
			Assert.isTrue(sendTo.value().length >= 1, "At least one outbound destination need to be provided.");
			return sendTo.value();
		}
		return null;
	}
}
