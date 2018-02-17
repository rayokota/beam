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

import java.util.Map;

import org.apache.beam.runners.kstream.translation.KafkaStreamsPipelineTranslator;
import org.apache.beam.runners.kstream.translation.KafkaStreamsTransformOverrides;
import org.apache.beam.runners.kstream.translation.PViewToIdMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PValue;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} that executes the operations in the
 * pipeline by first translating them to the Kafka Streams API
 * and then executing them on a Kafka cluster.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class KafkaStreamsRunner extends PipelineRunner<KafkaStreamsPipelineResult> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsRunner.class);

  public static KafkaStreamsRunner fromOptions(PipelineOptions opts) {
    final KafkaStreamsPipelineOptions kafkaStreamsOptions = (KafkaStreamsPipelineOptions) opts;
    return new KafkaStreamsRunner(kafkaStreamsOptions);
  }

  private final KafkaStreamsPipelineOptions options;

  public KafkaStreamsRunner(KafkaStreamsPipelineOptions options) {
    this.options = options;
  }

  @Override
  public KafkaStreamsPipelineResult run(Pipeline pipeline) {
    try {
      pipeline.replaceAll(KafkaStreamsTransformOverrides.getDefaultOverrides());

      // Add a dummy source for use in special cases (TestStream, empty flatten)
      final PValue dummySource = pipeline.apply("Dummy Input Source", Create.of("dummy"));

      final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);

      StreamsBuilder streamsBuilder = new StreamsBuilder();
      streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
          Stores.persistentKeyValueStore("beamStore"),
          Serdes.ByteArray(),
          Serdes.ByteArray()
      ).withCachingEnabled());
      KafkaStreamsPipelineTranslator.translate(pipeline, options, streamsBuilder, idMap, dummySource);
      KafkaStreams streams = new KafkaStreams(
          streamsBuilder.build(), options.getStreamsConfiguration());
      final KafkaStreamsPipelineResult result = new KafkaStreamsPipelineResult(streams);

      streams.start();

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
