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

package org.apache.beam.runners.kstream.translation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.beam.runners.kstream.AbstractIntegrationTest;
import org.apache.beam.runners.kstream.KafkaStreamsPipelineResult;
import org.apache.beam.runners.kstream.io.KafkaStreamsIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test for {@link GroupByKeyTranslator}.
 */
public class GroupByKeyTranslatorTest extends AbstractIntegrationTest {
  protected static final String INPUT_TOPIC = "inputTopic";
  protected static final String OUTPUT_TOPIC = "outputTopic";

  protected final List<Input<String>> input = Arrays.asList(
      new Input<>(INPUT_TOPIC, "foo", 1000),
      new Input<>(INPUT_TOPIC, "foo", 1000),
      new Input<>(INPUT_TOPIC, "foo", 2000),
      new Input<>(INPUT_TOPIC, "bar", 1000),
      new Input<>(INPUT_TOPIC, "bar", 2000),
      new Input<>(INPUT_TOPIC, "bar", 2000),
      new Input<>(INPUT_TOPIC, "zap", 3000)
  );

  @Override
  protected String[] getTopics() {
    return new String[]{INPUT_TOPIC, OUTPUT_TOPIC};
  }

  @Before
  public void prepareTopology() throws InterruptedException {
    super.prepareEnvironment();

    appID = "groupbykey-translator-test";
  }

  @SuppressWarnings({"unchecked"})
  @Test
  public void test() throws Exception {
    STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-groupbykey");
    Pipeline p = getPipeline();

    // expected results assume outputAtLatestInputTimestamp
    List<KV<Instant, KV<String, Long>>> expected =
        Lists.newArrayList(
            KV.of(new Instant(1000), KV.of("foo", 2L)),
            KV.of(new Instant(1000), KV.of("bar", 1L)),
            KV.of(new Instant(2000), KV.of("foo", 1L)),
            KV.of(new Instant(2000), KV.of("bar", 2L))
        );

    p.apply(KafkaStreamsIO.<String, String>read()
        .withTopics(ImmutableList.of(INPUT_TOPIC))
        .withKeySerdeAndCoder(new Serdes.StringSerde(), StringUtf8Coder.of())
        .withValueSerdeAndCoder(new Serdes.StringSerde(), StringUtf8Coder.of()))
        .apply(ParDo.of(new RemoveKey<>()))
        .apply(
            Window.<String>into(FixedWindows.of(Duration.standardSeconds(1)))
                .withTimestampCombiner(TimestampCombiner.LATEST))
        .apply(Count.perElement())
        .apply(ParDo.of(new KeyedByTimestamp<>()))
        .apply(ParDo.of(new EmbeddedCollector()));


    KafkaStreamsPipelineResult result = (KafkaStreamsPipelineResult) p.run();

    long ts = System.currentTimeMillis();
    for (final Input<String> singleInput : input) {
      producer.send(new ProducerRecord<>(
          singleInput.topic, null, singleInput.timestamp, singleInput.record.key, singleInput.record.value)).get();
    }

    long timeout = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < timeout) {
      if (EmbeddedCollector.RESULTS.containsAll(expected)) {
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertEquals(Sets.newHashSet(expected), EmbeddedCollector.RESULTS);

  }

  private static class RemoveKey<K, V> extends DoFn<KV<K, V>, V> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element().getValue());
    }
  }

  private static class EmbeddedCollector extends DoFn<Object, Void> {
    private static final Set<Object> RESULTS = Collections.synchronizedSet(new HashSet<>());

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      RESULTS.add(c.element());
    }
  }

  private static class KeyedByTimestamp<T> extends DoFn<T, KV<Instant, T>> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(KV.of(c.timestamp(), c.element()));
    }
  }

}
