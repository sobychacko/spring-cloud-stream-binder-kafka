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

package org.springframework.cloud.stream.binder.kafka.streams.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.cloud.stream.binding.AbstractBindableProxyFactory;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.binding.BoundTargetHolder;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.core.ResolvableType;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Kafka Streams specific target bindings proxy factory. See {@link AbstractBindableProxyFactory} for more details.
 *
 * Targets bound by this factory:
 *
 * {@link KStream}
 * {@link KTable}
 * {@link GlobalKTable}
 *
 * This class looks at the Function bean's return signature as {@link ResolvableType} and introspect the individual types,
 * binding them on the way.
 *
 * All types on the {@link ResolvableType} are bound except for KStream[] array types on the outbound, which will be
 * deferred for binding at a later stage. The reason for doing that is because in this class, we don't have any way to know
 * the actual size in the returned array. That has to wait until the function is invoked and we get a result.
 *
 * @author Soby Chacko
 * @since 3.0.0
 */
public class KafkaStreamsBindableProxyFactory extends AbstractBindableProxyFactory implements InitializingBean, BeanFactoryAware {

	/**
	 * Default output binding name. Output binding may occur later on in the function invoker (outside of this class),
	 * thus making this field part of the API.
	 */
	public static final String DEFAULT_OUTPUT_SUFFIX = "out";
	private static final String DEFAULT_INPUT_SUFFIX = "in";

	private static Log log = LogFactory.getLog(BindableProxyFactory.class);

	@Autowired
	private StreamFunctionProperties streamFunctionProperties;

	private final ResolvableType type;

	private final String functionName;

	private BeanFactory beanFactory;


	public KafkaStreamsBindableProxyFactory(ResolvableType type, String functionName) {
		super(type.getType().getClass());
		this.type = type;
		this.functionName = functionName;
	}

	@Override
	public void afterPropertiesSet() {
		Assert.notEmpty(KafkaStreamsBindableProxyFactory.this.bindingTargetFactories,
				"'bindingTargetFactories' cannot be empty");

		int resolvableTypeDepthCounter = 0;
		ResolvableType argument = this.type.getGeneric(resolvableTypeDepthCounter++);
		List<String> inputBindings = buildInputBindings();
		Iterator<String> iterator = inputBindings.iterator();
		String next = iterator.next();
		bindInput(argument, next);

		if (this.type.getRawClass() != null &&
				(this.type.getRawClass().isAssignableFrom(BiFunction.class) ||
				this.type.getRawClass().isAssignableFrom(BiConsumer.class))) {
			argument = this.type.getGeneric(resolvableTypeDepthCounter++);
			next = iterator.next();
			bindInput(argument, next);
		}
		ResolvableType outboundArgument = this.type.getGeneric(resolvableTypeDepthCounter);

		while (isAnotherFunctionOrConsumerFound(outboundArgument)) {
			//The function is a curried function. We should introspect the partial function chain hierarchy.
			argument = outboundArgument.getGeneric(0);
			String next1 = iterator.next();
			bindInput(argument, next1);
			outboundArgument = outboundArgument.getGeneric(1);
		}

		//Introspect output for binding.
		if (outboundArgument != null &&  outboundArgument.getRawClass() != null && (!outboundArgument.isArray() &&
				outboundArgument.getRawClass().isAssignableFrom(KStream.class))) {
			// if the type is array, we need to do a late binding as we don't know the number of
			// output bindings at this point in the flow.

			List<String> outputBindings = streamFunctionProperties.getOutputBindings().get(this.functionName);
			String outputBinding = null;

			if (!CollectionUtils.isEmpty(outputBindings)) {
				Iterator<String> outputBindingsIter = outputBindings.iterator();
				if (outputBindingsIter.hasNext()) {
					outputBinding = outputBindingsIter.next();
				}

			}
			else {
				outputBinding = String.format("%s_%s", this.functionName, DEFAULT_OUTPUT_SUFFIX);
			}
			Assert.isTrue(outputBinding != null, "output binding is not inferred.");
			KafkaStreamsBindableProxyFactory.this.outputHolders.put(outputBinding,
					new BoundTargetHolder(getBindingTargetFactory(KStream.class)
							.createOutput(outputBinding), true));
			String outputBinding1 = outputBinding;
			RootBeanDefinition rootBeanDefinition1 = new RootBeanDefinition();
			rootBeanDefinition1.setInstanceSupplier(() -> outputHolders.get(outputBinding1).getBoundTarget());
			BeanDefinitionRegistry registry = (BeanDefinitionRegistry) beanFactory;
			registry.registerBeanDefinition(outputBinding1, rootBeanDefinition1);
		}
	}

	private boolean isAnotherFunctionOrConsumerFound(ResolvableType arg1) {
		return arg1 != null && !arg1.isArray() && arg1.getRawClass() != null &&
				(arg1.getRawClass().isAssignableFrom(Function.class) || arg1.getRawClass().isAssignableFrom(Consumer.class));
	}

	/**
	 * If the application provides the property spring.cloud.stream.function.inputBindings.functionName,
	 * that gets precedence. Otherwise, use functionName-input or functionName-input-0, functionName-input-1 and so on
	 * for multiple inputs.
	 *
	 * @return an ordered collection of input bindings to use
	 */
	private List<String> buildInputBindings() {
		List<String> inputs = new ArrayList<>();
		List<String> inputBindings = streamFunctionProperties.getInputBindings().get(this.functionName);
		if (!CollectionUtils.isEmpty(inputBindings)) {
			inputs.addAll(inputBindings);
			return inputs;
		}
		int numberOfInputs = this.type.getRawClass() != null &&
				(this.type.getRawClass().isAssignableFrom(BiFunction.class) ||
						this.type.getRawClass().isAssignableFrom(BiConsumer.class)) ? 2 : getNumberOfInputs();
		if (numberOfInputs == 1) {
			inputs.add(String.format("%s_%s", this.functionName, DEFAULT_INPUT_SUFFIX));
			return inputs;
		}
		else {
			int i = 0;
			while (i < numberOfInputs) {
				inputs.add(String.format("%s_%s_%d", this.functionName, DEFAULT_INPUT_SUFFIX, i++));
			}
			return inputs;
		}
	}

	private int getNumberOfInputs() {
		int numberOfInputs = 1;
		ResolvableType arg1 = this.type.getGeneric(1);

		while (isAnotherFunctionOrConsumerFound(arg1)) {
			arg1 = arg1.getGeneric(1);
			numberOfInputs++;
		}
		return numberOfInputs;

	}

	private void bindInput(ResolvableType arg0, String inputName) {
		if (arg0.getRawClass() != null) {
			KafkaStreamsBindableProxyFactory.this.inputHolders.put(inputName,
					new BoundTargetHolder(getBindingTargetFactory(arg0.getRawClass())
							.createInput(inputName), true));
		}

		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) beanFactory;

		RootBeanDefinition rootBeanDefinition = new RootBeanDefinition();
		rootBeanDefinition.setInstanceSupplier(() -> inputHolders.get(inputName).getBoundTarget());
		registry.registerBeanDefinition(inputName, rootBeanDefinition);

	}

	@Override
	public Set<String> getInputs() {
		Set<String> ins = new LinkedHashSet<>();
		this.inputHolders.forEach((s, BoundTargetHolder) -> ins.add(s));
		return ins;
	}

	@Override
	public Set<String> getOutputs() {
		Set<String> outs = new LinkedHashSet<>();
		this.outputHolders.forEach((s, BoundTargetHolder) -> outs.add(s));
		return outs;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	public void addOutputBinding(String output, Class<?> clazz) {
		KafkaStreamsBindableProxyFactory.this.outputHolders.put(output,
				new BoundTargetHolder(getBindingTargetFactory(clazz)
						.createOutput(output), true));
	}

	public Map<String, BoundTargetHolder> getOutputHolders() {
		return outputHolders;
	}
}

