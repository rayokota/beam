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

import java.util.Properties;
import java.util.UUID;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.kafka.streams.StreamsConfig;
import org.joda.time.Duration;

/**
 * Kafka Streams {@link PipelineRunner} for testing.
 */
public class TestKafkaStreamsRunner extends PipelineRunner<KafkaStreamsPipelineResult> {

  private static final int RUN_WAIT_MILLIS = 60000;
  private final KafkaStreamsRunner delegate;

  private TestKafkaStreamsRunner(KafkaStreamsPipelineOptions options) {
    Properties streamsConfig = new Properties();
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-runner-" + UUID.randomUUID());
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    options.setStreamsConfiguration(streamsConfig);
    this.delegate = KafkaStreamsRunner.fromOptions(options);
  }

  public static TestKafkaStreamsRunner fromOptions(PipelineOptions options) {
    KafkaStreamsPipelineOptions kafkaStreamsOptions = PipelineOptionsValidator
        .validate(KafkaStreamsPipelineOptions.class, options);
    return new TestKafkaStreamsRunner(kafkaStreamsOptions);
  }

  @Override
  public KafkaStreamsPipelineResult run(Pipeline pipeline) {
    try {
      final KafkaStreamsPipelineResult result = delegate.run(pipeline);
      // this is necessary for tests that just call run() and not waitUntilFinish
      result.waitUntilFinish(Duration.millis(RUN_WAIT_MILLIS));
      return result;
    } catch (Throwable t) {
      // Search for AssertionError. If present use it as the cause of the pipeline failure.
      Throwable current = t;

      while (current != null) {
        if (current instanceof AssertionError) {
          throw (AssertionError) current;
        }
        current = current.getCause();
      }

      throw t;
    }
  }

}
