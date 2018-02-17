/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.kstream;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.kafka.streams.KafkaStreams;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Result of executing a {@link Pipeline} with Kafka Streams.
 */
public class KafkaStreamsPipelineResult implements PipelineResult {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsPipelineResult.class);

  private final AtomicReference<StateInfo> stateRef =
      new AtomicReference<>(new StateInfo(State.STOPPED));

  private final KafkaStreams.StateListener stateListener = new StateListener();
  private final UncaughtExceptionHandler exceptionHandler = new UncaughtExceptionHandler();

  private final KafkaStreams streams;

  public KafkaStreamsPipelineResult(KafkaStreams streams) {
    this.streams = streams;
    streams.setStateListener(stateListener);
    streams.setUncaughtExceptionHandler(exceptionHandler);
  }

  @Override
  public State getState() {
    return stateRef.get().state;
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException("Cancellation is not supported");
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    long timeout = (duration == null || duration.getMillis() < 1) ? Long.MAX_VALUE
        : System.currentTimeMillis() + duration.getMillis();
    StateInfo currentState;
    try {
      do {
        currentState = stateRef.get();
        if (currentState.error != null) {
          throw currentState.error;
        }
        Thread.sleep(500);
      } while (!currentState.state.isTerminal() && System.currentTimeMillis() < timeout);

      streams.close();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    if (!currentState.state.isTerminal()) {
      markSuccess();
    }

    return currentState.state;
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(null);
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException(
        String.format("%s does not support querying metrics", getClass().getSimpleName()));
  }

  public void markStarted() {
    StateInfo currentState;
    do {
      currentState = stateRef.get();
      if (currentState.state != State.STOPPED) {
        LOG.warn(
            "Invalid state transition from {} to RUNNING. "
                + "Only valid transition is from STOPPED. Ignoring.",
            currentState.state
        );
      }
    } while (!stateRef.compareAndSet(currentState, new StateInfo(State.RUNNING)));
  }

  public void markSuccess() {
    StateInfo currentState;
    do {
      currentState = stateRef.get();
      if (currentState.state != State.RUNNING) {
        LOG.warn(
            "Invalid state transition from {} to DONE. "
                + "Only valid transition is from RUNNING. Ignoring. ",
            currentState.state
        );
      }
    } while (!stateRef.compareAndSet(currentState, new StateInfo(State.DONE)));

    streams.close();
  }

  public void markFailure(Throwable error) {
    final Pipeline.PipelineExecutionException wrappedException =
        new Pipeline.PipelineExecutionException(error);

    StateInfo currentState;
    do {
      currentState = stateRef.get();
      if (currentState.state != State.RUNNING) {
        LOG.warn(
            "Invalid state transition from {} to FAILED. "
                + "Only valid transition is from RUNNING. Ignoring. ",
            currentState.state
        );
      }
    } while (!stateRef.compareAndSet(currentState, new StateInfo(State.FAILED, wrappedException)));

    streams.close();
  }

  private class StateListener implements KafkaStreams.StateListener {
    @Override
    public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
      switch (newState) {
        case RUNNING:
          markStarted();
          break;
        case NOT_RUNNING:
          markSuccess();
          break;
      }
    }
  }

  private class UncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      markFailure(e);
    }
  }

  private static class StateInfo {
    private final State state;
    private final Pipeline.PipelineExecutionException error;

    private StateInfo(State state) {
      this(state, null);
    }

    private StateInfo(State state, Pipeline.PipelineExecutionException error) {
      this.state = state;
      this.error = error;
    }
  }

}
