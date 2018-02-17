package org.apache.beam.runners.kstream.io;

import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.joda.time.Instant;

public class KVTransformer<K, V>
    implements Transformer<K, KV<K, V>, KeyValue<K, WindowedValue<KV<K, V>>>> {

  private transient ProcessorContext context;
  private long lastMaxTimestamp = Long.MIN_VALUE;
  private long currentMaxTimestamp;

  public static <K, V> TransformerSupplier<
      K, KV<K, V>, KeyValue<K, WindowedValue<KV<K, V>>>> supplier() {
    return new TransformerSupplier<K, KV<K, V>, KeyValue<K, WindowedValue<KV<K, V>>>>() {
      @Override
      public Transformer<K, KV<K, V>, KeyValue<K, WindowedValue<KV<K, V>>>> get() {
        return new KVTransformer<>();
      }
    };
  }

  @Override
  public final void init(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public KeyValue<K, WindowedValue<KV<K, V>>> transform(K key, KV<K, V> kv) {
    long timestamp = context.timestamp();
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
    return new KeyValue<>(
        key,
        WindowedValue.of(kv, new Instant(timestamp),
            GlobalWindow.INSTANCE, PaneInfo.NO_FIRING
        )
    );
  }

  @Override
  public KeyValue<K, WindowedValue<KV<K, V>>> punctuate(final long timestamp) {
    return null;
  }

  @Override
  public void close() {
  }
}
