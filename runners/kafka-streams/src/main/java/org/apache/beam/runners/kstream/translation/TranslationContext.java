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
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.kstream.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kstream.io.BoundedSourceAdapter;
import org.apache.beam.runners.kstream.io.ValuesSource;
import org.apache.beam.runners.kstream.io.WindowedValueSerde;
import org.apache.beam.runners.kstream.runtime.OpMessage;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains context data for {@link TransformTranslator}s.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TranslationContext {
  private static final Logger LOG = LoggerFactory.getLogger(TranslationContext.class);

  private final StreamsBuilder streamsBuilder;
  private final Map<PValue, KStream<?, ?>> messageStreams = new HashMap<>();
  private final Map<PCollectionView<?>, KStream<?, ?>> viewStreams = new HashMap<>();
  private final Map<PValue, String> idMap;
  private final Map<PValue, Integer> shortIdMap = new HashMap<>();
  private final KafkaStreamsPipelineOptions options;
  private final PValue dummySource;

  private AppliedPTransform<?, ?, ?> currentTransform;
  private int topologicalId;

  public TranslationContext(
      StreamsBuilder streamsBuilder,
      Map<PValue, String> idMap,
      KafkaStreamsPipelineOptions options,
      PValue dummySource
  ) {
    this.streamsBuilder = streamsBuilder;
    this.idMap = idMap;
    this.options = options;
    this.dummySource = dummySource;
    int i = 0;
    for (PValue pvalue : idMap.keySet()) {
      shortIdMap.put(pvalue, i++);
    }
  }

  public <K, OutT> KStream<K, OutT> getInputStream(
      Collection<String> topics,
      Serde<K> keySerde,
      Serde<OutT> valueSerde
  ) {
    PValue output = getOutput();
    return streamsBuilder.stream(topics, Consumed.with(keySerde, valueSerde))
        .peek((k, v) -> log(null, output, "input", v));
  }

  public <K, OutT> void registerMessageStream(
      PValue pvalue,
      KStream<K, OutT> stream
  ) {
    if (pvalue.equals(dummySource)) {
      return;
    }
    if (messageStreams.containsKey(pvalue)) {
      throw new IllegalArgumentException("Stream already registered for pvalue: " + pvalue);
    }

    messageStreams.put(pvalue, stream);
  }

  public <K, OutT> KStream<K, OutT> getMessageStream(PValue pvalue) {
    @SuppressWarnings("unchecked")
    KStream<K, OutT> stream =
        (KStream<K, OutT>) messageStreams.get(pvalue);
    if (stream == null) {
      throw new IllegalArgumentException("No stream registered for pvalue: " + pvalue);
    }
    stream = stream.peek((k, v) -> log(null, null, "msg", v));
    return stream;
  }

  public <K, OutT> KStream<K, OutT> getDummyStream() {
    if (!messageStreams.containsKey(dummySource)) {
      // create a dummy source that never emits anything
      Coder<OutT> coder = (Coder<OutT>) VoidCoder.of();
      BoundedSource<OutT> boundedSource = new ValuesSource<>(Collections.emptyList(), coder);
      BoundedSourceAdapter<OutT> adapter = new BoundedSourceAdapter<>(boundedSource, getPipelineOptions(), coder);
      adapter.start();
      try {
        adapter.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        adapter.close();
      }
      KStream<byte[], WindowedValue<OutT>> stream = getInputStream(
          ImmutableList.of(adapter.getTopic()),
          new Serdes.ByteArraySerde(), new WindowedValueSerde<>(coder)
      );

      messageStreams.put(dummySource, stream);
    }

    return getMessageStream(dummySource);
  }

  public <K, ElemT, ViewT> void registerViewStream(
      PCollectionView<ViewT> view,
      KStream<K, OpMessage<ElemT>> stream
  ) {
    if (viewStreams.containsKey(view)) {
      throw new IllegalArgumentException("Stream already registered for view: " + view);
    }

    viewStreams.put(view, stream);
  }

  public <K, InT> KStream<K, OpMessage<InT>> getViewStream(PCollectionView<?> view) {
    @SuppressWarnings("unchecked")
    KStream<K, OpMessage<InT>> stream =
        (KStream<K, OpMessage<InT>>) viewStreams.get(view);
    if (stream == null) {
      throw new IllegalArgumentException("No stream registered for view: " + view);
    }
    PValue input = getInput();
    stream = stream.peek((k, v) -> log(input, null, "view", v));
    return stream;
  }

  public <ViewT> String getViewId(PCollectionView<ViewT> view) {
    return getIdForPValue(view);
  }

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }

  public void clearCurrentTransform() {
    this.currentTransform = null;
  }

  /**
   * Uniquely identify a node when doing a topological traversal of the BEAM
   * {@link org.apache.beam.sdk.Pipeline}. It's changed on a per-node basis.
   *
   * @param id id for the node.
   */
  public void setCurrentTopologicalId(int id) {
    this.topologicalId = id;
  }

  public int getCurrentTopologicalId() {
    return this.topologicalId;
  }

  @SuppressWarnings("unchecked")
  public <InT extends PValue> InT getInput() {
    return (InT) Iterables.getOnlyElement(
        TransformInputs.nonAdditionalInputs(this.currentTransform));
  }

  @SuppressWarnings("unchecked")
  public <OutT extends PValue> OutT getOutput() {
    return (OutT) Iterables.getOnlyElement(this.currentTransform.getOutputs().values());
  }

  @SuppressWarnings("unchecked")
  public <OutT> TupleTag<OutT> getOutputTag(PTransform<?, ? extends PCollection<OutT>> transform) {
    return (TupleTag<OutT>) Iterables.getOnlyElement(this.currentTransform.getOutputs().keySet());
  }

  public KafkaStreamsPipelineOptions getPipelineOptions() {
    return this.options;
  }


  public void log(PValue invalue, PValue outvalue, String context, Object value) {
    int in = invalue != null ? getShortIdForPValue(invalue) : -1;
    int out = outvalue != null ? getShortIdForPValue(outvalue) : -1;
    LOG.debug("in: {}, out: {}, ctx: {}, val: {}", in, out, context, value);
  }

  public String getIdForPValue(PValue pvalue) {
    final String id = idMap.get(pvalue);
    if (id == null) {
      throw new IllegalArgumentException("No id mapping for value: " + pvalue);
    }
    return id;
  }

  public int getShortIdForPValue(PValue pvalue) {
    final Integer id = shortIdMap.get(pvalue);
    if (id == null) {
      throw new IllegalArgumentException("No short id mapping for value: " + pvalue);
    }
    return id;
  }
}
