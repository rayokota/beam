/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.kstream.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.beam.runners.kstream.runtime.ControlMessage;
import org.apache.beam.runners.kstream.runtime.ControlMessage.Type;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.kafka.common.serialization.Deserializer;
import org.joda.time.Instant;

public class WindowedValueDeserializer<T> implements Deserializer<WindowedValue<T>> {
  private static final int TYPE_SIZE = 1;
  private static final int TIMESTAMP_SIZE = 8;

  private Coder<T> inner;

  public WindowedValueDeserializer(final Coder<T> inner) {
    this.inner = inner;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public WindowedValue<T> deserialize(String topic, byte[] data) {

    Type type = Type.valueOf(data[0]);
    long timestamp = ByteBuffer.wrap(data).getLong(TYPE_SIZE);

    if (type != Type.ELEMENT) {
      return ControlMessage.of(type, new Instant(timestamp));
    } else {

      byte[] bytes = new byte[data.length - TYPE_SIZE - TIMESTAMP_SIZE];
      System.arraycopy(data, TYPE_SIZE + TIMESTAMP_SIZE, bytes, 0, bytes.length);

      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      T value;
      try {
        value = inner.decode(bis);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return WindowedValue.of(value, new Instant(timestamp),
          GlobalWindow.INSTANCE, PaneInfo.NO_FIRING
      );
    }
  }

  @Override
  public void close() {
  }
}
