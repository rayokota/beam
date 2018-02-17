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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.beam.runners.kstream.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kstream.runtime.ControlMessage;
import org.apache.beam.runners.kstream.runtime.ControlMessage.Type;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoundedSourceAdapter<T> extends KafkaStreamsSourceAdapter<T> {

  private static <T> List<BoundedSource<T>> split(
      BoundedSource<T> source,
      KafkaStreamsPipelineOptions pipelineOptions
  ) throws Exception {
    final int numSplits = pipelineOptions.getMaxSourceParallelism();
    if (numSplits > 1) {
      final long estimatedSize = source.getEstimatedSizeBytes(pipelineOptions);
      // calculate the size of each split, rounded up to the ceiling.
      final long bundleSize = (estimatedSize + numSplits - 1) / numSplits;
      @SuppressWarnings("unchecked") final List<BoundedSource<T>> splits = (List<BoundedSource<T>>)
          source.split(bundleSize, pipelineOptions);
      if (!splits.isEmpty()) {
        return splits;
      }
    }
    return Collections.singletonList(source);
  }

  private static final Logger LOG = LoggerFactory.getLogger(BoundedSourceAdapter.class);
  private static final AtomicInteger NEXT_ID = new AtomicInteger();

  private final List<BoundedSource<T>> splits;
  private final KafkaStreamsPipelineOptions pipelineOptions;
  private final List<BoundedReader<T>> readers = new ArrayList<>();

  private ReaderTask readerTask;
  private Thread readerThread;

  public BoundedSourceAdapter(
      BoundedSource<T> source,
      KafkaStreamsPipelineOptions pipelineOptions,
      Coder<T> coder
  ) {
    super(source.getClass().getSimpleName().toLowerCase(), pipelineOptions, coder);
    try {
      splits = split(source, pipelineOptions);
    } catch (Exception e) {
      throw new StreamsException("Fail to split source", e);
    }
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public void start() {
    for (int i = 0; i < splits.size(); i++) {
      register(i);
    }

    readerTask = new ReaderTask(readers);
    readerThread = new Thread(
        readerTask,
        "bounded-source-system-consumer-" + NEXT_ID.getAndIncrement()
    );
    readerThread.start();
  }

  @Override
  public void join() throws InterruptedException {
    if (readerTask != null) {
      readerThread.join();
    }
  }

  @Override
  public void stop() {
    // NOTE: this is not a blocking shutdown
    if (readerTask != null) {
      readerTask.stop();
    }
  }

  private void register(int index) {
    try {
      final BoundedReader<T> reader = splits.get(index).createReader(pipelineOptions);
      readers.add(reader);
    } catch (Exception e) {
      throw new StreamsException("Error while creating source reader", e);
    }
  }

  private class ReaderTask implements Runnable {
    private final List<BoundedReader<T>> readers;

    // NOTE: we do not support recovery with a bounded source (we restart from the beginning),
    // so we do not need to have a way to tie an offset to a position in the bounded source.
    private long offset;
    private volatile Thread readerThread;
    private volatile boolean stopInvoked = false;
    private volatile Exception lastException;

    private ReaderTask(List<BoundedReader<T>> readers) {
      this.readers = readers;
    }

    @Override
    public void run() {
      readerThread = Thread.currentThread();

      final Set<BoundedReader<T>> availableReaders = new HashSet<>(readers);
      try {
        for (BoundedReader<T> reader : readers) {
          boolean hasData = reader.start();
          if (hasData) {
            enqueueMessage(reader);
          } else {
            reader.close();
            availableReaders.remove(reader);
          }
        }

        while (!stopInvoked && !availableReaders.isEmpty()) {
          final Iterator<BoundedReader<T>> iter = availableReaders.iterator();
          while (iter.hasNext()) {
            final BoundedReader<T> reader = iter.next();
            final boolean hasData = reader.advance();
            if (hasData) {
              enqueueMessage(reader);
            } else {
              reader.close();
              iter.remove();
            }
          }
        }
        enqueueMaxWatermarkAndEndOfStream();
      } catch (InterruptedException e) {
        // We use an interrupt to wake the reader from a blocking read under normal termination,
        // so ignore it here.
      } catch (Exception e) {
        setError(e);
      } finally {
        availableReaders.forEach(reader -> {
          try {
            reader.close();
          } catch (IOException e) {
            LOG.error("Reader task failed to close reader", e);
          }
        });
      }
    }

    private void enqueueMessage(BoundedReader<T> reader) throws InterruptedException {
      final T value = reader.getCurrent();
      final WindowedValue<T> windowedValue = WindowedValue.timestampedValueInGlobalWindow(
          value,
          reader.getCurrentTimestamp()
      );
      enqueue(windowedValue);
    }

    private void enqueueMaxWatermarkAndEndOfStream() {
      // Send the max watermark to force completion of any open windows.
      final ControlMessage watermarkEnvelope = ControlMessage.of(
          Type.WATERMARK,
          BoundedWindow.TIMESTAMP_MAX_VALUE
      );
      enqueue(watermarkEnvelope);

      final ControlMessage endOfStreamEnvelope = ControlMessage.of(
          Type.END_OF_STREAM,
          BoundedWindow.TIMESTAMP_MAX_VALUE
      );
      enqueue(endOfStreamEnvelope);
    }

    private void stop() {
      stopInvoked = true;

      final Thread readerThread = this.readerThread;
      if (readerThread != null) {
        readerThread.interrupt();
      }
    }

    private void setError(Exception exception) {
      // TODO handle exception
      this.lastException = exception;
    }
  }
}
