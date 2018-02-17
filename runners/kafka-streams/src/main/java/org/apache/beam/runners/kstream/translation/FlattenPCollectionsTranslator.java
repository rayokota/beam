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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.kstream.runtime.ControlMessage;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.joda.time.Instant;

/**
 * Translates {@link org.apache.beam.sdk.transforms.Flatten.PCollections} to merge operator.
 */
class FlattenPCollectionsTranslator<T>
    implements TransformTranslator<Flatten.PCollections<T>> {
  @Override
  public void translate(
      Flatten.PCollections<T> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx
  ) {
    final PCollection<T> output = ctx.getOutput();

    final List<KStream<Object, WindowedValue<T>>> inputStreams = new ArrayList<>();
    for (Map.Entry<TupleTag<?>, PValue> taggedPValue : node.getInputs().entrySet()) {
      if (!(taggedPValue.getValue() instanceof PCollection)) {
        throw new IllegalArgumentException(
            String.format(
                "Got non-PCollection input for flatten. Tag: %s. Input: %s. Type: %s",
                taggedPValue.getKey(),
                taggedPValue.getValue(),
                taggedPValue.getValue().getClass()
            ));
      }

      @SuppressWarnings("unchecked") final PCollection<T> input = (PCollection<T>) taggedPValue.getValue();
      inputStreams.add(ctx.getMessageStream(input));
    }

    if (inputStreams.size() == 0) {
      final KStream<?, WindowedValue<T>> noOpStream =
          ctx.getDummyStream().flatMap((k, v) -> Collections.emptyList());
      ctx.registerMessageStream(output, noOpStream);
      return;
    }

    if (inputStreams.size() == 1) {
      ctx.registerMessageStream(output, inputStreams.get(0));
      return;
    }

    KStream<Object, WindowedValue<T>> outputStream = flattenWithWatermarkManager(inputStreams);
    ctx.registerMessageStream(output, outputStream);
  }

  private static <T> KStream<Object, WindowedValue<T>> flattenWithWatermarkManager(
      List<KStream<Object, WindowedValue<T>>> inputStreams
  ) {
    final WatermarkManager watermarkManager = new WatermarkManager(inputStreams.size());

    KStream<Object, WindowedValue<T>> outputStream = null;
    final Set<KStream<Object, WindowedValue<T>>> streamsToMerge = new HashSet<>();
    for (int i = 0; i < inputStreams.size(); i++) {
      final int index = i;
      KStream<Object, WindowedValue<T>> stream = inputStreams.get(i)
          .mapValues(v -> {
            if (v instanceof ControlMessage) ((ControlMessage) v).setIndex(index);
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

  static class ProcessWatermark<T> implements ValueMapper<WindowedValue<T>, Iterable<WindowedValue<T>>> {

    private WatermarkManager watermarkManager;

    public ProcessWatermark(WatermarkManager watermarkManager) {
      this.watermarkManager = watermarkManager;
    }

    @Override
    public Iterable<WindowedValue<T>> apply(final WindowedValue<T> value) {
      if (!(value instanceof ControlMessage)) {
        return ImmutableList.of(value);
      }
      ControlMessage controlMessage = (ControlMessage) value;
      if (controlMessage.getType() != ControlMessage.Type.WATERMARK) {
        return ImmutableList.of(value);
      }
      Instant outputWatermark = watermarkManager.getOutputWatermark(
          controlMessage.getIndex(), controlMessage.getTimestamp());
      if (outputWatermark != null) {
        return ImmutableList.of(ControlMessage.of(ControlMessage.Type.WATERMARK, outputWatermark));
      } else {
        return Collections.emptyList();
      }
    }
  }

}
