package org.springframework.cloud.stream.binder.kstream;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.StreamsBuilderFactoryBean;
import org.springframework.util.Assert;

/**
 * @author Soby Chacko
 */
public class CustomizedStreamsBuilderFactoryBean extends AbstractFactoryBean<StreamsBuilder> implements SmartLifecycle {

	private static final int DEFAULT_CLOSE_TIMEOUT = 10;

	private StreamsConfig streamsConfig;

	private KafkaStreams kafkaStreams;

	private KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

	private boolean autoStartup = true;

	private int phase = Integer.MIN_VALUE;

	private KafkaStreams.StateListener stateListener;

	private Thread.UncaughtExceptionHandler exceptionHandler;

	private int closeTimeout = DEFAULT_CLOSE_TIMEOUT;

	private volatile boolean running;

	public void setStreamsConfig(StreamsConfig streamsConfig) {
		this.streamsConfig = streamsConfig;
	}

	public void setClientSupplier(KafkaClientSupplier clientSupplier) {
		Assert.notNull(clientSupplier, "'clientSupplier' must not be null");
		this.clientSupplier = clientSupplier;
	}

	public void setStateListener(KafkaStreams.StateListener stateListener) {
		this.stateListener = stateListener;
	}

	public void setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}

	/**
	 * Specify the timeout in seconds for the {@link KafkaStreams#close(long, TimeUnit)} operation.
	 * Defaults to 10 seconds.
	 * @param closeTimeout the timeout for close in seconds.
	 * @see KafkaStreams#close(long, TimeUnit)
	 */
	public void setCloseTimeout(int closeTimeout) {
		this.closeTimeout = closeTimeout;
	}

	@Override
	public Class<?> getObjectType() {
		return StreamsBuilder.class;
	}

	@Override
	protected StreamsBuilder createInstance() throws Exception {
		return new StreamsBuilder();
	}


	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		if (callback != null) {
			callback.run();
		}
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
			try {
				this.kafkaStreams = new KafkaStreams(getObject().build(), this.streamsConfig, this.clientSupplier);
				this.kafkaStreams.setStateListener(this.stateListener);
				this.kafkaStreams.setUncaughtExceptionHandler(this.exceptionHandler);
				this.kafkaStreams.start();
				this.running = true;
			}
			catch (Exception e) {
				throw new KafkaException("Could not start stream: ", e);
			}
		}
	}

	@Override
	public synchronized void stop() {
		if (this.running) {
			try {
				if (this.kafkaStreams != null) {
					this.kafkaStreams.close(this.closeTimeout, TimeUnit.SECONDS);
					this.kafkaStreams.cleanUp();
					this.kafkaStreams = null;
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			finally {
				this.running = false;
			}
		}
	}

	@Override
	public synchronized boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	/**
	 * Get a managed by this {@link StreamsBuilderFactoryBean} {@link KafkaStreams} instance.
	 * @return KafkaStreams managed instance;
	 * may be null if this {@link StreamsBuilderFactoryBean} hasn't been started.
	 * @since 1.1.4
	 */
	public KafkaStreams getKafkaStreams() {
		return this.kafkaStreams;
	}



}
