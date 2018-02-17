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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.kstream.runtime.ControlMessage;
import org.apache.beam.runners.kstream.runtime.DoFnOp;
import org.apache.beam.runners.kstream.runtime.DoFnOpAdapter;
import org.apache.beam.runners.kstream.runtime.OpMessage;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.joda.time.Instant;

/**
 * Translates {@link org.apache.beam.sdk.transforms.ParDo.MultiOutput} to Kafka Streams {@link DoFnOp}.
 */
public class ParDoBoundMultiTranslator<InT, OutT>
    implements TransformTranslator<ParDo.MultiOutput<InT, OutT>> {

  @Override
  public void translate(
      ParDo.MultiOutput<InT, OutT> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx
  ) {
    final PCollection<? extends InT> input = ctx.getInput();

    final DoFnSignature signature = DoFnSignatures.getSignature(transform.getFn().getClass());
    if (signature.usesState()) {
      throw new UnsupportedOperationException("DoFn with state is not currently supported");
    }

    if (signature.usesTimers()) {
      throw new UnsupportedOperationException("DoFn with timers is not currently supported");
    }

    if (signature.processElement().isSplittable()) {
      throw new UnsupportedOperationException("Splittable DoFn is not currently supported");
    }

    // TODO: verify windowing strategy is bounded window!
    @SuppressWarnings("unchecked") final WindowingStrategy<? extends InT, BoundedWindow> windowingStrategy =
        (WindowingStrategy<? extends InT, BoundedWindow>) input.getWindowingStrategy();

    KStream<Object, WindowedValue<InT>> msgStream = ctx.getMessageStream(input);
    msgStream = msgStream.peek((k, v) -> ctx.log(input, null, "msgstream", v));
    KStream<Object, OpMessage<InT>> inputStream = msgStream.mapValues(elem -> {
      return elem instanceof ControlMessage
          ? OpMessage.ofControl((ControlMessage) elem)
          : OpMessage.ofElement(elem);
    });
    inputStream = inputStream.peek((k, v) -> ctx.log(input, null, "inputstream", v));
    final List<KStream<Object, OpMessage<InT>>> sideInputStreams = new ArrayList<>();
    for (PCollectionView<?> view : transform.getSideInputs()) {
      KStream<Object, OpMessage<InT>> sideInputStream = ctx.getViewStream(view);
      sideInputStream = sideInputStream.peek((k, v) -> ctx.log(input, null, "sideinputstream", v));
      sideInputStreams.add(sideInputStream);
    }

    final Map<TupleTag<?>, Integer> tagToIdMap = new HashMap<>();
    final Map<Integer, PCollection<?>> idToPCollectionMap = new HashMap<>();
    final List<Coder<?>> unionCoderElements = new ArrayList<>();
    ArrayList<Map.Entry<TupleTag<?>, PValue>> outputs =
        new ArrayList<>(node.getOutputs().entrySet());
    for (int id = 0; id < outputs.size(); ++id) {
      final Map.Entry<TupleTag<?>, PValue> taggedOutput = outputs.get(id);
      tagToIdMap.put(taggedOutput.getKey(), id);

      if (!(taggedOutput.getValue() instanceof PCollection)) {
        throw new IllegalArgumentException("Expected side output to be PCollection, but was: "
            + taggedOutput.getValue());
      }
      final PCollection<?> sideOutputCollection = (PCollection<?>) taggedOutput.getValue();
      unionCoderElements.add(sideOutputCollection.getCoder());

      idToPCollectionMap.put(id, sideOutputCollection);
    }

    final Map<String, PCollectionView<?>> idToPValueMap = new HashMap<>();
    for (PCollectionView<?> view : transform.getSideInputs()) {
      idToPValueMap.put(ctx.getViewId(view), view);
    }

    final DoFnOp<InT, OutT, RawUnionValue> op = new DoFnOp<>(
        ctx.getPipelineOptions(),
        transform.getMainOutputTag(),
        transform.getFn(),
        transform.getSideInputs(),
        transform.getAdditionalOutputTags().getAll(),
        input.getWindowingStrategy(),
        idToPValueMap,
        new DoFnOp.MultiOutputManagerFactory(tagToIdMap),
        node.getFullName()
    );

    KStream<Object, OpMessage<InT>> mergedStreams;
    if (sideInputStreams.size() == 0) {
      mergedStreams = inputStream;
    } else {
      List<KStream<Object, OpMessage<InT>>> inputStreams = new ArrayList<>();
      inputStreams.add(inputStream);
      inputStreams.addAll(sideInputStreams);
      mergedStreams = flattenWithWatermarkManager(inputStreams);
    }
    mergedStreams = mergedStreams.peek((k, v) -> ctx.log(input, null, "mergedstreams", v));
    KStream<Object, WindowedValue<RawUnionValue>> taggedOutputStream =
        mergedStreams
            .transform(DoFnOpAdapter.adapt(op), "beamStore")
            .flatMapValues(m -> m);
    taggedOutputStream = taggedOutputStream.peek((k, v) -> ctx.log(input, null, "taggedoutputstreams", v));

    for (int outputIndex : tagToIdMap.values()) {
      final Coder<WindowedValue<OutT>> outputCoder =
          WindowedValue.FullWindowedValueCoder.of(
              (Coder<OutT>) idToPCollectionMap.get(outputIndex).getCoder(),
              windowingStrategy.getWindowFn().windowCoder()
          );

      registerSideOutputStream(
          transform.getFn(),
          taggedOutputStream,
          idToPCollectionMap.get(outputIndex),
          outputCoder,
          outputIndex,
          ctx
      );
    }
  }

  private String debug(DoFn doFn, String stream, Object v) {
    return "** " + doFn.getClass().getName() + " " + stream + ": " + v;
  }

  private <T> void registerSideOutputStream(
      DoFn<?, ?> doFn,
      KStream<?, WindowedValue<RawUnionValue>> inputStream,
      PValue outputPValue,
      Coder<T> coder,
      int outputIndex,
      TranslationContext ctx
  ) {

    @SuppressWarnings("unchecked")
    KStream<?, WindowedValue<T>> outputStream = inputStream
        .filter(new FilterByUnionId(outputIndex))
        .mapValues(new RawUnionValueToValue(coder));

    outputStream = outputStream.peek((k, v) -> ctx.log(null, outputPValue, "sideoutputstream", v));
    ctx.registerMessageStream(outputPValue, outputStream);
  }

  public static class FilterByUnionId<K> implements Predicate<K, WindowedValue<RawUnionValue>> {
    private final int id;

    public FilterByUnionId(int id) {
      this.id = id;
    }

    @Override
    public boolean test(K key, WindowedValue<RawUnionValue> message) {
      if (message instanceof ControlMessage) {
        return true;
      } else {
        RawUnionValue value = message.getValue();
        return value.getUnionTag() == id;
      }
    }
  }

  public static class RawUnionValueToValue<OutT> implements ValueMapper<
      WindowedValue<RawUnionValue>, WindowedValue<OutT>> {
    private final Coder<WindowedValue<OutT>> coder;

    private RawUnionValueToValue(Coder<WindowedValue<OutT>> coder) {
      this.coder = coder;
    }

    @Override
    public WindowedValue<OutT> apply(WindowedValue<RawUnionValue> inputElement) {
      if (inputElement instanceof ControlMessage) {
        return (ControlMessage) inputElement;
      } else {
        @SuppressWarnings("unchecked")
        OutT value = (OutT) inputElement.getValue().getValue();
        return inputElement.withValue(value);
      }
    }
  }

  private static <T> KStream<Object, OpMessage<T>> flattenWithWatermarkManager(
      List<KStream<Object, OpMessage<T>>> inputStreams
  ) {
    final WatermarkManager watermarkManager = new WatermarkManager(inputStreams.size());

    KStream<Object, OpMessage<T>> outputStream = null;
    final Set<KStream<Object, OpMessage<T>>> streamsToMerge = new HashSet<>();
    for (int i = 0; i < inputStreams.size(); i++) {
      final int index = i;
      KStream<Object, OpMessage<T>> stream = inputStreams.get(i)
          .mapValues(v -> {
            if (v.getType() == OpMessage.Type.CONTROL) v.getControlMessage().setIndex(index);
            return v;
          });
      boolean inserted = streamsToMerge.add(stream);
      if (!inserted) {
        // Merge same streams. Make a copy of the current stream.
        outputStream = outputStream.merge(stream.mapValues(m -> m));
      } else {
        if (outputStream == null) {
          outputStream = stream;
        } else {
          outputStream = outputStream.merge(stream);
        }
      }
    }
    outputStream = outputStream.flatMapValues(new ProcessWatermark<>(watermarkManager));
    return outputStream;
  }

  static class ProcessWatermark<T> implements ValueMapper<OpMessage<T>, Iterable<OpMessage<T>>> {

    private WatermarkManager watermarkManager;

    public ProcessWatermark(WatermarkManager watermarkManager) {
      this.watermarkManager = watermarkManager;
    }

    @Override
    public Iterable<OpMessage<T>> apply(final OpMessage<T> value) {
      if (value.getType() != OpMessage.Type.CONTROL) {
        return ImmutableList.of(value);
      }
      ControlMessage controlMessage = value.getControlMessage();
      if (controlMessage.getType() != ControlMessage.Type.WATERMARK) {
        return ImmutableList.of(value);
      }
      Instant outputWatermark = watermarkManager.getOutputWatermark(
          controlMessage.getIndex(), controlMessage.getTimestamp());
      if (outputWatermark != null) {
        return ImmutableList.of(OpMessage.ofControl(ControlMessage.of(ControlMessage.Type.WATERMARK, outputWatermark)));
      } else {
        return Collections.emptyList();
      }
    }
  }
}
