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
package org.apache.beam.runners.kstream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class AbstractIntegrationTest {
  @ClassRule
  public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

  @Rule
  public final TemporaryFolder testFolder = new TemporaryFolder(TestUtils.tempDirectory());

  protected static String appID;

  private static final Long COMMIT_INTERVAL = 100L;
  protected static final Properties STREAMS_CONFIG = new Properties();
  private final long anyUniqueKey = 0L;

  private static final Properties PRODUCER_CONFIG = new Properties();

  protected KafkaProducer<Long, String> producer;

  protected AbstractIntegrationTest() {
  }

  @BeforeClass
  public static void setupConfigsAndUtils() {
    PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all");
    PRODUCER_CONFIG.put(ProducerConfig.RETRIES_CONFIG, 0);
    PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    STREAMS_CONFIG.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
    STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
    STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);

    STREAMS_CONFIG.put(StreamsConfig.producerPrefix(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), LongSerializer.class);
    STREAMS_CONFIG.put(StreamsConfig.producerPrefix(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), StringSerializer.class);
  }

  protected void prepareEnvironment() throws InterruptedException {
    String[] topics = getTopics();
    if (topics.length != 0) {
      CLUSTER.createTopics(topics);
    }

    STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath());

    producer = new KafkaProducer<>(PRODUCER_CONFIG);
  }

  protected abstract String[] getTopics();

  protected Pipeline getPipeline() {
    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.as(KafkaStreamsPipelineOptions.class);
    options.setRunner(KafkaStreamsRunner.class);
    options.setStreamsConfiguration(STREAMS_CONFIG);
    return Pipeline.create(options);
  }

  @After
  public void cleanup() throws InterruptedException {
    CLUSTER.deleteTopicsAndWait(120000, getTopics());
  }

  public final class Input<V> {
    public String topic;
    public KeyValue<Long, V> record;
    public long timestamp = 0L;

    public Input(final String topic, final V value) {
      this.topic = topic;
      this.record = KeyValue.pair(anyUniqueKey, value);
    }

    public Input(final String topic, final V value, final long timestamp) {
      this.topic = topic;
      this.record = KeyValue.pair(anyUniqueKey, value);
      this.timestamp = timestamp;
    }
  }

  public static class EmbeddedCollector extends DoFn<Object, Void> {
    public static final List<Object> RESULTS = Collections.synchronizedList(new ArrayList<>());

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      RESULTS.add(((KV) c.element()).getValue());
    }
  }
}
