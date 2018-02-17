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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.beam.runners.kstream.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kstream.runtime.ControlMessage;
import org.apache.beam.runners.kstream.runtime.ControlMessage.Type;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnboundedSourceAdapter<T, CheckpointMarkT extends CheckpointMark> extends KafkaStreamsSourceAdapter<T> {

  private static <T, CheckpointMarkT extends CheckpointMark>
  List<UnboundedSource<T, CheckpointMarkT>> split(
      UnboundedSource<T, CheckpointMarkT> source,
      KafkaStreamsPipelineOptions pipelineOptions
  ) throws Exception {
    final int numSplits = pipelineOptions.getMaxSourceParallelism();
    if (numSplits > 1) {
      @SuppressWarnings("unchecked") final List<UnboundedSource<T, CheckpointMarkT>> splits =
          (List<UnboundedSource<T, CheckpointMarkT>>) source.split(numSplits, pipelineOptions);
      if (!splits.isEmpty()) {
        return splits;
      }
    }
    return Collections.singletonList(source);
  }

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedSourceAdapter.class);

  private static final AtomicInteger NEXT_ID = new AtomicInteger();

  private final List<UnboundedSource<T, CheckpointMarkT>> splits;
  private final KafkaStreamsPipelineOptions pipelineOptions;
  private final List<UnboundedReader> readers = new ArrayList<>();

  private ReaderTask readerTask;
  private Thread readerThread;

  public UnboundedSourceAdapter(
      UnboundedSource<T, CheckpointMarkT> source,
      KafkaStreamsPipelineOptions pipelineOptions,
      Coder<T> coder
  ) {
    super(source.getClass().getSimpleName().toLowerCase(), pipelineOptions, coder);
    try {
      this.splits = split(source, pipelineOptions);
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

    readerTask = new ReaderTask(readers, pipelineOptions.getWatermarkInterval());
    readerThread = new Thread(
        readerTask,
        "unbounded-source-system-consumer-" + NEXT_ID.getAndIncrement()
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
    readerTask.stop();
  }

  private void register(int index) {
    try {
      final UnboundedReader reader = splits.get(index)
          .createReader(pipelineOptions, null);
      readers.add(reader);
    } catch (Exception e) {
      throw new StreamsException("Error while creating source reader", e);
    }
  }

  private class ReaderTask implements Runnable {
    private final List<UnboundedReader> readers;
    private final Instant[] currentWatermarks;
    private final long watermarkInterval;

    private volatile boolean running;
    private volatile Exception lastException;
    private long lastWatermarkTime = 0L;

    private ReaderTask(
        List<UnboundedReader> readers,
        long watermarkInterval
    ) {
      this.readers = readers;
      this.currentWatermarks = new Instant[readers.size()];
      this.watermarkInterval = watermarkInterval;
    }

    @Override
    public void run() {
      this.running = true;

      try {
        for (UnboundedReader reader : readers) {
          final boolean hasData = reader.start();
          if (hasData) {
            enqueueMessage(reader);
          }
        }

        while (running) {
          boolean elementAvailable = false;
          for (UnboundedReader reader : readers) {
            final boolean hasData = reader.advance();
            if (hasData) {
              updateWatermark();
              enqueueMessage(reader);
              elementAvailable = true;
            }
          }

          updateWatermark();

          if (!elementAvailable) {
            //TODO: make poll interval configurable
            Thread.sleep(10);
          }
        }
      } catch (Exception e) {
        lastException = e;
        running = false;
      } finally {
        readers.forEach(reader -> {
          try {
            reader.close();
          } catch (IOException e) {
            LOG.error("Reader task failed to close reader", e);
          }
        });
      }
    }

    private void updateWatermark() throws InterruptedException, IOException {
      final long time = System.currentTimeMillis();
      if (time - lastWatermarkTime > watermarkInterval) {
        for (int i = 0; i < readers.size(); i++) {
          UnboundedReader reader = readers.get(i);
          Instant currentWatermark = currentWatermarks[i];
          if (currentWatermark == null) {
            currentWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
          }
          final Instant nextWatermark = reader.getWatermark();
          if (currentWatermark.isBefore(nextWatermark)) {
            currentWatermarks[i] = nextWatermark;
            enqueueWatermark(reader);
          }
        }

        lastWatermarkTime = time;
      }
    }

    private void enqueueWatermark(UnboundedReader reader)
        throws InterruptedException, IOException {
      final WindowedValue<T> windowedValue = ControlMessage.of(
          Type.WATERMARK, reader.getWatermark());

      enqueue(windowedValue);
    }

    private void enqueueMessage(UnboundedReader reader)
        throws InterruptedException, IOException {
      @SuppressWarnings("unchecked") final T value = (T) reader.getCurrent();
      final Instant time = reader.getCurrentTimestamp();
      final WindowedValue<T> windowedValue = WindowedValue.timestampedValueInGlobalWindow(
          value,
          time
      );

      enqueue(windowedValue);
    }

    void stop() {
      running = false;
    }
  }
}
