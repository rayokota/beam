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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.runners.kstream.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kstream.io.KafkaStreamsIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KafkaStreamsPipelineTranslator} knows how to translate {@link Pipeline} objects
 * into Kafka Streams.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class KafkaStreamsPipelineTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsPipelineTranslator.class);

  private static final Map<Class<? extends PTransform>, TransformTranslator<?>> TRANSLATORS;

  static {
    Map<Class<? extends PTransform>, TransformTranslator<?>> translators = new HashMap<>();
    translators.put(Read.Bounded.class, new ReadBoundedTranslator<>());
    translators.put(Read.Unbounded.class, new ReadUnboundedTranslator<>());
    translators.put(ParDo.MultiOutput.class, new ParDoBoundMultiTranslator<>());
    translators.put(GroupByKey.class, new GroupByKeyTranslator<>());
    translators.put(Window.Assign.class, new WindowAssignTranslator<>());
    translators.put(Flatten.PCollections.class, new FlattenPCollectionsTranslator<>());
    translators.put(KafkaStreamsPublishView.class, new KafkaStreamsPublishViewTranslator<>());
    translators.put(KafkaStreamsIO.Read.class, new KafkaStreamsIOReadTranslator<>());
    translators.put(KafkaStreamsIO.Write.class, new KafkaStreamsIOWriteTranslator<>());
    TRANSLATORS = Collections.unmodifiableMap(translators);
  }

  private KafkaStreamsPipelineTranslator() {
  }

  public static void translate(
      Pipeline pipeline,
      KafkaStreamsPipelineOptions options,
      StreamsBuilder builder,
      Map<PValue, String> idMap,
      PValue dummySource
  ) {
    final TranslationContext ctx = new TranslationContext(builder, idMap, options, dummySource);
    final TranslationVisitor visitor = new TranslationVisitor(ctx);
    pipeline.traverseTopologically(visitor);
  }

  private static class TranslationVisitor extends Pipeline.PipelineVisitor.Defaults {
    private final TranslationContext ctx;
    private int topologicalId = 0;

    private TranslationVisitor(TranslationContext ctx) {
      this.ctx = ctx;
    }

    /**
     * Returns the {@link TransformTranslator} to use for instances of the
     * specified PTransform class, or null if none registered.
     */
    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      final PTransform<?, ?> transform = node.getTransform();

      Class<? extends PTransform> transformClass = transform.getClass();
      if (KafkaStreamsIO.Read.class.isAssignableFrom(transformClass)) {
        transformClass = KafkaStreamsIO.Read.class;
      } else if (KafkaStreamsIO.Write.class.isAssignableFrom(transformClass)) {
        transformClass = KafkaStreamsIO.Write.class;
      }
      final TransformTranslator<?> translator = TRANSLATORS.get(transformClass);
      if (translator == null) {
        throw new UnsupportedOperationException(
            String.format("Unsupported transform class: %s. Node: %s", transform, node));
      }

      ctx.setCurrentTransform(node.toAppliedPTransform(getPipeline()));
      ctx.setCurrentTopologicalId(topologicalId++);
      applyTransform(transform, node, translator);
      ctx.clearCurrentTransform();
    }

    private <T extends PTransform<?, ?>> void applyTransform(
        T transform,
        TransformHierarchy.Node node,
        TransformTranslator<?> translator
    ) {
      @SuppressWarnings("unchecked") final TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;
      typedTranslator.translate(transform, node, ctx);
    }
  }
}
