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

import org.apache.beam.runners.kstream.io.KafkaStreamsIO;
import org.apache.beam.runners.kstream.runtime.ControlMessage;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Flatten.FlattenPCollectionList is translated to Kafka Streams merge function.
 */
public class KafkaStreamsIOWriteTranslator<K, V> implements
    TransformTranslator<KafkaStreamsIO.Write<K, V>> {

  private static final long serialVersionUID = 8145377766484976114L;

  @Override
  public void translate(
      KafkaStreamsIO.Write<K, V> transform, TransformHierarchy.Node node, TranslationContext ctx
  ) {
    String topic = transform.getTopic();
    final PCollection<KV<K, V>> input = ctx.getInput();
    final KStream<?, WindowedValue<KV<K, V>>> inputStream = ctx.getMessageStream(input);
    inputStream
        .filter((k, wv) -> !(wv instanceof ControlMessage))
        .map((k, wv) -> new KeyValue<>(wv.getValue().getKey(), wv.getValue().getValue()))
        .to(topic, Produced.<K, V>with(transform.<K>getKeySerde(), transform.<V>getValueSerde()));
  }

}
