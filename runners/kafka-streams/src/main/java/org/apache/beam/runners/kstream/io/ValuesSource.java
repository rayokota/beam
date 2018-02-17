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

package org.apache.beam.runners.kstream.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;

/**
 * Unbounded source that reads from a Java {@link Iterable}.
 */
public class ValuesSource<T> extends BoundedSource<T> {
  private static final long serialVersionUID = 1L;

  private final byte[] codedValues;
  private final IterableCoder<T> iterableCoder;

  public ValuesSource(Iterable<T> values, Coder<T> coder) {
    this.iterableCoder = IterableCoder.of(coder);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      iterableCoder.encode(values, bos, Context.OUTER);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    this.codedValues = bos.toByteArray();
  }

  @Override
  public java.util.List<? extends BoundedSource<T>> split(
      long desiredNumSplits, PipelineOptions options
  ) throws Exception {
    return Collections.singletonList(this);
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return 0L;
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) {
    ByteArrayInputStream bis = new ByteArrayInputStream(codedValues);
    try {
      Iterable<T> values = this.iterableCoder.decode(bis, Context.OUTER);
      return new ValuesReader<>(values, this);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Coder<T> getOutputCoder() {
    return iterableCoder.getElemCoder();
  }

  private static class ValuesReader<T> extends BoundedReader<T> {

    private final Iterable<T> values;
    private final BoundedSource<T> source;
    private transient Iterator<T> iterator;
    private T current;

    public ValuesReader(Iterable<T> values, BoundedSource<T> source) {
      this.values = values;
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      if (null == iterator) {
        iterator = values.iterator();
      }
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return Instant.now();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public BoundedSource<T> getCurrentSource() {
      return source;
    }
  }
}
