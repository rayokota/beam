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

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.joda.time.Instant;

/**
 * Interface of Kafka Streams operator for BEAM. This interface demultiplexes messages from BEAM
 * so that elements and side inputs can be handled separately. Watermark propagation
 * can be overridden so we can hold watermarks for side inputs. The output values and watermark
 * will be collected via {@link OpEmitter}.
 */
public interface Op<InT, OutT> extends Serializable {

  default void open(
      ProcessorContext context,
      OpEmitter<OutT> emitter
  ) {
  }

  void processElement(
      WindowedValue<InT> inputElement,
      OpEmitter<OutT> emitter
  );

  default void processWatermark(
      Instant watermark,
      OpEmitter<OutT> emitter
  ) {
    emitter.emitWatermark(watermark);
  }

  default void close() {
  }
}
