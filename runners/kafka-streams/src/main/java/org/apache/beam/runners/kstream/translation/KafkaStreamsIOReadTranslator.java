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

import org.apache.beam.runners.kstream.io.KVTransformer;
import org.apache.beam.runners.kstream.io.KafkaStreamsIO;
import org.apache.beam.runners.kstream.io.WatermarkGenerator;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

public class KafkaStreamsIOReadTranslator<K, V> implements
    TransformTranslator<KafkaStreamsIO.Read<K, V>> {

  private static final long serialVersionUID = 8145377766484976114L;

  @Override
  public void translate(
      KafkaStreamsIO.Read<K, V> transform, TransformHierarchy.Node node, TranslationContext ctx
  ) {
    KStream<K, WindowedValue<KV<K, V>>> stream = ctx.getInputStream(transform.getTopics(),
        transform.getKeySerde(), transform.getValueSerde()
    )
        .map((k, v) -> new KeyValue<>(k, KV.of(k, v)))
        .transform(KVTransformer.supplier())
        .transform(WatermarkGenerator.supplier(
            transform.getWatermarkGenerationInterval(),
            transform.getWatermarkGenerationType()
        ));
    ctx.registerMessageStream(ctx.getOutput(), stream);
  }
}
