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

import java.util.List;

import org.apache.beam.runners.kstream.runtime.ControlMessage;
import org.apache.beam.runners.kstream.runtime.OpMessage;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Translates {@link KafkaStreamsPublishView} to a view as side input.
 */
class KafkaStreamsPublishViewTranslator<ElemT, ViewT>
    implements TransformTranslator<KafkaStreamsPublishView<ElemT, ViewT>> {
  @Override
  public void translate(
      KafkaStreamsPublishView<ElemT, ViewT> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx
  ) {
    final PCollection<List<ElemT>> input = ctx.getInput();

    final KStream<?, WindowedValue<Iterable<ElemT>>> inputStream = ctx.getMessageStream(input);

    final KStream<?, OpMessage<ElemT>> outputStream =
        inputStream
            .mapValues(wv -> {
              ctx.log(input, null, "view", wv);
              return wv instanceof ControlMessage
                  ? OpMessage.ofControl((ControlMessage) wv)
                  : OpMessage.ofSideInput(ctx.getViewId(transform.getView()), wv);
            });

    ctx.registerViewStream(transform.getView(), outputStream);
  }
}
