/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.kstream.translation;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.runners.kstream.AbstractIntegrationTest;
import org.apache.beam.runners.kstream.KafkaStreamsPipelineResult;
import org.apache.beam.runners.kstream.io.KafkaStreamsIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FlattenPCollectionsTranslatorTest extends AbstractIntegrationTest {
  protected static final String INPUT_TOPIC_RIGHT = "inputTopicRight";
  protected static final String INPUT_TOPIC_LEFT = "inputTopicLeft";
  protected static final String OUTPUT_TOPIC = "outputTopic";

  protected final List<Input<String>> input = Arrays.asList(
      new Input<>(INPUT_TOPIC_LEFT, (String) null),
      new Input<>(INPUT_TOPIC_RIGHT, (String) null),
      new Input<>(INPUT_TOPIC_LEFT, "A"),
      new Input<>(INPUT_TOPIC_RIGHT, "a"),
      new Input<>(INPUT_TOPIC_LEFT, "B"),
      new Input<>(INPUT_TOPIC_RIGHT, "b"),
      new Input<>(INPUT_TOPIC_LEFT, (String) null),
      new Input<>(INPUT_TOPIC_RIGHT, (String) null),
      new Input<>(INPUT_TOPIC_LEFT, "C"),
      new Input<>(INPUT_TOPIC_RIGHT, "c"),
      new Input<>(INPUT_TOPIC_RIGHT, (String) null),
      new Input<>(INPUT_TOPIC_LEFT, (String) null),
      new Input<>(INPUT_TOPIC_RIGHT, (String) null),
      new Input<>(INPUT_TOPIC_RIGHT, "d"),
      new Input<>(INPUT_TOPIC_LEFT, "D")
  );

  @Override
  protected String[] getTopics() {
    return new String[]{INPUT_TOPIC_RIGHT, INPUT_TOPIC_LEFT, OUTPUT_TOPIC};
  }

  @Before
  public void prepareTopology() throws InterruptedException {
    super.prepareEnvironment();

    appID = "flatten-translator-test";
  }

  @Test
  public void testFlatten() throws Exception {
    STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-flatten");
    Pipeline p = getPipeline();

    String[] expected = {null, null, "A", "a", "B", "b", null, null, "C", "c", null, null, null, "d", "D"};

    List<PCollection<KV<String, String>>> pcList = new ArrayList<>();
    pcList.add(p.apply(
        KafkaStreamsIO.<String, String>read()
            .withTopics(ImmutableList.of(INPUT_TOPIC_LEFT))
            .withKeySerdeAndCoder(new Serdes.StringSerde(), StringUtf8Coder.of())
            .withValueSerdeAndCoder(new Serdes.StringSerde(), StringUtf8Coder.of())));
    pcList.add(p.apply(
        KafkaStreamsIO.<String, String>read()
            .withTopics(ImmutableList.of(INPUT_TOPIC_RIGHT))
            .withKeySerdeAndCoder(new Serdes.StringSerde(), StringUtf8Coder.of())
            .withValueSerdeAndCoder(new Serdes.StringSerde(), StringUtf8Coder.of())));

    p.apply(
        KafkaStreamsIO.<String, String>read()
            .withTopics(ImmutableList.of(OUTPUT_TOPIC))
            .withKeySerdeAndCoder(new Serdes.StringSerde(), StringUtf8Coder.of())
            .withValueSerdeAndCoder(new Serdes.StringSerde(), StringUtf8Coder.of()))
        .apply(ParDo.of(new EmbeddedCollector()));

    PCollection<KV<String, String>> actual =
        PCollectionList.of(pcList).apply(Flatten.pCollections());
    actual.apply(KafkaStreamsIO.<String, String>write().withTopic(OUTPUT_TOPIC)
        .withKeySerde(new Serdes.StringSerde())
        .withValueSerde(new Serdes.StringSerde()));

    KafkaStreamsPipelineResult result = (KafkaStreamsPipelineResult) p.run();

    long ts = System.currentTimeMillis();
    for (final Input<String> singleInput : input) {
      producer.send(new ProducerRecord<>(
          singleInput.topic, null, ++ts, singleInput.record.key, singleInput.record.value)).get();
    }

    long timeout = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < timeout
        && EmbeddedCollector.RESULTS.size() < expected.length) {
      Thread.sleep(500);
    }

    Assert.assertEquals(Arrays.asList(expected), EmbeddedCollector.RESULTS);
    Assert.assertEquals("number results", expected.length, EmbeddedCollector.RESULTS.size());
  }
}
