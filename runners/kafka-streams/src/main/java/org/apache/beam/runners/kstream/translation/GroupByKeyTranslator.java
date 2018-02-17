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

import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.runners.kstream.runtime.DoFnOp;
import org.apache.beam.runners.kstream.runtime.GroupByKeyOp;
import org.apache.beam.runners.kstream.runtime.KvToKeyedWorkItemOp;
import org.apache.beam.runners.kstream.runtime.OpAdapter;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.kafka.streams.kstream.KStream;

/**
 */
class GroupByKeyTranslator<K, V>
    implements TransformTranslator<GroupByKey<K, V>> {

  @Override
  public void translate(
      GroupByKey<K, V> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx
  ) {
    final PCollection<KV<K, V>> input = ctx.getInput();

    final PCollection<KV<K, Iterable<V>>> output = ctx.getOutput();
    final TupleTag<KV<K, Iterable<V>>> outputTag = ctx.getOutputTag(transform);

    @SuppressWarnings("unchecked") final WindowingStrategy<?, BoundedWindow> windowingStrategy =
        (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();

    final KStream<?, WindowedValue<KV<K, V>>> inputStream = ctx.getMessageStream(input);

    final Coder<KV<K, V>> inputCoder = input.getCoder();
    if (!(inputCoder instanceof KvCoder)) {
      throw new IllegalStateException("Could not get KvCoder from input coder: " + inputCoder);
    }
    final KvCoder<K, V> kvInputCoder = (KvCoder<K, V>) inputCoder;
    final Coder<WindowedValue<KV<K, V>>> elementCoder =
        WindowedValue.FullWindowedValueCoder.of(
            inputCoder,
            windowingStrategy.getWindowFn().windowCoder()
        );

    final KStream<?, WindowedValue<KV<K, V>>> filteredInputStream = inputStream
        .peek((k, v) -> ctx.log(input, output, "filteredInputStream", v));

    final KStream<?, WindowedValue<KV<K, V>>> partitionedInputStream;
    /* TODO perform repartitioning?
    if (ctx.getPipelineOptions().getMaxSourceParallelism() == 1) {
      // Only one task will be created, no need for repartition
      partitionedInputStream = filteredInputStream;
    } else {
      partitionedInputStream = filteredInputStream
          .partitionBy(msg -> msg.getElement().getValue().getKey(), msg -> msg.getElement(),
              KVSerde.of(coderToSerde(kvInputCoder.getKeyCoder()), coderToSerde(elementCoder)),
              "gbk-" + ctx.getCurrentTopologicalId())
          .map(kv -> OpMessage.ofElement(kv.getValue()));
    }
    */
    partitionedInputStream = filteredInputStream;

    final Coder<KeyedWorkItem<K, V>> keyedWorkItemCoder =
        KeyedWorkItemCoder.of(
            kvInputCoder.getKeyCoder(),
            kvInputCoder.getValueCoder(),
            windowingStrategy.getWindowFn().windowCoder()
        );

    final KStream<?, WindowedValue<KV<K, Iterable<V>>>> outputStream =
        partitionedInputStream
            .peek((k, v) -> ctx.log(input, output, "partitioned1", v))
            .transform(OpAdapter.adapt(new KvToKeyedWorkItemOp<>()), "beamStore")
            .peek((k, v) -> ctx.log(input, output, "partitioned2", v))
            .flatMapValues(m -> m)
            .peek((k, v) -> ctx.log(input, output, "partitioned3", v))
            .transform(OpAdapter.adapt(new GroupByKeyOp<>(
                outputTag,
                keyedWorkItemCoder,
                windowingStrategy,
                new DoFnOp.SingleOutputManagerFactory<>(),
                node.getFullName()
            )), "beamStore")
            .peek((k, v) -> ctx.log(input, output, "partitioned4", v))
            .flatMapValues(m -> m)
            .peek((k, v) -> ctx.log(input, output, "partitioned5", v));

    ctx.registerMessageStream(output, outputStream);
  }

  private String debug(String stream, Object v) {
    return "** " + " " + stream + ": " + v;
  }
}
