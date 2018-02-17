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

package org.apache.beam.runners.kstream.io;

import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.UUID;

import org.apache.beam.runners.kstream.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaStreamsSourceAdapter<T> implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsSourceAdapter.class);

  private final KafkaStreamsPipelineOptions pipelineOptions;
  private final Coder<T> coder;
  private final String clientId;
  private final String topic;
  private final StreamsConfig streamsConfig;
  private final KafkaProducer producer;

  public KafkaStreamsSourceAdapter(String name, KafkaStreamsPipelineOptions pipelineOptions, Coder<T> coder) {
    this.pipelineOptions = pipelineOptions;
    this.coder = coder;
    this.clientId = name + "-" + UUID.randomUUID();
    this.topic = clientId + "-source";
    LOG.debug("creating topic {}", topic);
    this.streamsConfig = new StreamsConfig(pipelineOptions.getStreamsConfiguration());
    NewTopic newTopic = new NewTopic(topic,
        pipelineOptions.getNumPartitions(), pipelineOptions.getReplicationFactor()
    );
    AdminClient adminClient = AdminClient.create(pipelineOptions.getStreamsConfiguration());
    adminClient.createTopics(ImmutableList.of(newTopic));
    Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs(clientId);
    this.producer = new KafkaProducer(producerConfigs,
        new ByteArraySerializer(), new WindowedValueSerializer<>(coder)
    );
  }

  public String getTopic() {
    return topic;
  }

  public abstract void start();

  public abstract void join() throws InterruptedException;

  public abstract void stop();

  protected void enqueue(WindowedValue<T> envelope) {
    try {
      Long timestamp = envelope.getTimestamp().getMillis();
      if (timestamp < 0) {
        timestamp = null;
      }
      ProducerRecord producerRecord = new ProducerRecord(topic, null, timestamp, null, envelope);
      LOG.debug("sending topic {}, data {}", topic, envelope);
      producer.send(producerRecord);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    producer.flush();
    producer.close();
  }
}
