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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.beam.runners.kstream.runtime.ControlMessage;
import org.apache.beam.runners.kstream.runtime.ControlMessage.Type;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.kafka.common.serialization.Serializer;

public class WindowedValueSerializer<T> implements Serializer<WindowedValue<T>> {
  private static final int TYPE_SIZE = 1;
  private static final int TIMESTAMP_SIZE = 8;

  private Coder<T> inner;

  public WindowedValueSerializer(Coder<T> inner) {
    this.inner = inner;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, WindowedValue<T> data) {


    if (data instanceof ControlMessage) {
      ByteBuffer buf = ByteBuffer.allocate(TYPE_SIZE + TIMESTAMP_SIZE);
      ControlMessage.Type type = ((ControlMessage) data).getType();
      buf.put((byte) type.getValue());
      buf.putLong(data.getTimestamp().getMillis());
      return buf.array();
    } else {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try {
        inner.encode(data.getValue(), bos);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      byte[] value = bos.toByteArray();

      ByteBuffer buf = ByteBuffer.allocate(TYPE_SIZE + TIMESTAMP_SIZE + value.length);
      buf.put((byte) Type.ELEMENT.getValue());
      buf.putLong(data.getTimestamp().getMillis());
      buf.put(value);
      return buf.array();
    }
  }

  @Override
  public void close() {
  }
}
