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

import com.google.common.base.MoreObjects;
import com.google.common.collect.BiMap;
import com.google.common.collect.EnumHashBiMap;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

public class ControlMessage<T> extends WindowedValue<T> {

  public enum Type {
    ELEMENT(0),
    WATERMARK(1),
    END_OF_STREAM(2);

    private final int id;

    Type(int id) {
      this.id = id;
    }

    public int getValue() {
      return id;
    }

    private static final BiMap<Type, Integer> lookup = EnumHashBiMap.create(Type.class);

    static {
      for (Type type : Type.values()) {
        lookup.put(type, type.getValue());
      }
    }

    public static Type valueOf(int value) {
      return lookup.inverse().get(value);
    }
  }

  transient private int index;
  final private Type type;
  final private Instant timestamp;

  public static <T> ControlMessage<T> of(Type type, Instant timestamp) {
    return new ControlMessage<>(type, timestamp);
  }

  private ControlMessage(Type type, Instant timestamp) {
    this.type = type;
    this.timestamp = timestamp;
  }

  @Override
  public <NewT> WindowedValue<NewT> withValue(NewT value) {
    throw new UnsupportedOperationException();
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public Type getType() {
    return type;
  }

  @Override
  public T getValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public Collection<? extends BoundedWindow> getWindows() {
    return Collections.emptyList();
  }

  @Override
  public PaneInfo getPane() {
    return PaneInfo.NO_FIRING;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ControlMessage)) {
      return false;
    } else {
      ControlMessage that = (ControlMessage) other;

      // Compare timestamps first as they are most likely to differ.
      // Also compare timestamps according to millis-since-epoch because otherwise expensive
      // comparisons are made on their Chronology objects.
      return this.getType() == that.getType()
          && this.getTimestamp().isEqual(that.getTimestamp());
    }
  }

  @Override
  public int hashCode() {
    // Hash only the millis of the timestamp to be consistent with equals
    return Objects.hash(getType(), getTimestamp().getMillis());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("type", getType())
        .add("timestamp", getTimestamp())
        .toString();
  }
}
