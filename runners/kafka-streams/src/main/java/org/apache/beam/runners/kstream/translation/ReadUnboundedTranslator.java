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

import org.apache.beam.runners.kstream.io.UnboundedSourceAdapter;
import org.apache.beam.runners.kstream.io.WindowedValueSerde;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Translates {@link org.apache.beam.sdk.io.Read.Unbounded} to Kafka Streams input
 */
public class ReadUnboundedTranslator<T> implements TransformTranslator<Read.Unbounded<T>> {

  @Override
  public void translate(Read.Unbounded<T> transform, TransformHierarchy.Node node, TranslationContext ctx) {
    Coder<T> coder = ((PCollection) ctx.getOutput()).getCoder();
    UnboundedSource<T, ?> unboundedSource = transform.getSource();
    UnboundedSourceAdapter<T, ?> adapter = new UnboundedSourceAdapter<>(unboundedSource, ctx.getPipelineOptions(), coder);
    adapter.start();
    KStream<?, WindowedValue<T>> stream = ctx.getInputStream(ImmutableList.of(adapter.getTopic()),
        new Serdes.ByteArraySerde(), new WindowedValueSerde<>(coder)
    );
    ctx.registerMessageStream(ctx.getOutput(), stream);
  }
}