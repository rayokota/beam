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

package org.apache.beam.runners.kstream.runtime;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adaptor class that runs a Kafka Streams {@link Op} for BEAM.
 */
public class OpAdapter<K, InT, OutT>
    implements Transformer<K, WindowedValue<InT>, KeyValue<K, Collection<WindowedValue<OutT>>>>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(OpAdapter.class);

  private final Op<InT, OutT> op;
  private transient List<WindowedValue<OutT>> outputList;
  private transient OpEmitter<OutT> emitter;

  public static <K, InT, OutT> TransformerSupplier<
      K, WindowedValue<InT>, KeyValue<K, Collection<WindowedValue<OutT>>>> adapt(Op<InT, OutT> op) {
    return new TransformerSupplier<K, WindowedValue<InT>, KeyValue<K, Collection<WindowedValue<OutT>>>>() {
      @Override
      public Transformer<K, WindowedValue<InT>, KeyValue<K, Collection<WindowedValue<OutT>>>> get() {
        return new OpAdapter<>(op);
      }
    };
  }

  private OpAdapter(Op<InT, OutT> op) {
    this.op = op;
  }

  @Override
  public final void init(ProcessorContext context) {
    outputList = new ArrayList<>();
    emitter = new OpEmitterImpl();

    op.open(context, emitter);
  }

  @Override
  public KeyValue<K, Collection<WindowedValue<OutT>>> transform(K key, WindowedValue<InT> message) {
    try {
      if (message instanceof ControlMessage) {
        ControlMessage controlMessage = (ControlMessage) message;
        if (controlMessage.getType() == ControlMessage.Type.WATERMARK) {
          op.processWatermark(new Instant(controlMessage.getTimestamp()), emitter);
        }
      } else {
        op.processElement(message, emitter);
      }
    } catch (Exception e) {
      LOG.error(
          "Op {} threw an exception during processing",
          this.getClass().getName(),
          e
      );
      throw UserCodeException.wrap(e);
    }

    List<WindowedValue<OutT>> results = new ArrayList<WindowedValue<OutT>>(outputList);
    outputList.clear();
    return new KeyValue<>(key, results);
  }

  @Override
  public KeyValue<K, Collection<WindowedValue<OutT>>> punctuate(final long timestamp) {
    return null;
  }

  @Override
  public void close() {
    op.close();
  }

  private class OpEmitterImpl implements OpEmitter<OutT> {
    @Override
    public void emitElement(WindowedValue<OutT> element) {
      outputList.add(element);
    }

    @Override
    public void emitWatermark(Instant watermark) {
      outputList.add(ControlMessage.of(ControlMessage.Type.WATERMARK, watermark));
    }
  }
}
