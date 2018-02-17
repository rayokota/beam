package org.apache.beam.runners.kstream.io;

import org.apache.beam.runners.kstream.runtime.ControlMessage;
import org.apache.beam.runners.kstream.runtime.ControlMessage.Type;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.joda.time.Instant;

public class WatermarkGenerator<K, T>
    implements Transformer<K, WindowedValue<T>, KeyValue<K, WindowedValue<T>>> {

  private Instant lastMaxTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private Instant currentMaxTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private long interval;
  private PunctuationType type;

  public static <K, T> TransformerSupplier<
      K, WindowedValue<T>, KeyValue<K, WindowedValue<T>>> supplier(
      long watermarkGenerationInterval, PunctuationType watermarkGenerationType
  ) {
    return new TransformerSupplier<K, WindowedValue<T>, KeyValue<K, WindowedValue<T>>>() {
      @Override
      public Transformer<K, WindowedValue<T>, KeyValue<K, WindowedValue<T>>> get() {
        return new WatermarkGenerator<>(watermarkGenerationInterval, watermarkGenerationType);
      }
    };
  }

  public WatermarkGenerator(long interval, PunctuationType type) {
    this.interval = interval;
    this.type = type;
  }

  @Override
  public final void init(ProcessorContext context) {
    context.schedule(interval, type, new PunctuatorImpl(context));
  }

  @Override
  public KeyValue<K, WindowedValue<T>> transform(K key, WindowedValue<T> wv) {
    if (currentMaxTimestamp.isBefore(wv.getTimestamp())) {
      currentMaxTimestamp = wv.getTimestamp();
    }
    return new KeyValue<>(key, wv);
  }

  @Override
  public KeyValue<K, WindowedValue<T>> punctuate(final long timestamp) {
    return null;
  }

  @Override
  public void close() {
  }

  private class PunctuatorImpl implements Punctuator {

    // TODO make configurable
    private final long maxOutOfOrderness = 0;
    private ProcessorContext context;


    public PunctuatorImpl(ProcessorContext context) {
      this.context = context;
    }

    @Override
    public void punctuate(long timestamp) {
      if (lastMaxTimestamp != currentMaxTimestamp) {
        lastMaxTimestamp = currentMaxTimestamp;
        Instant watermark = new Instant(new Instant(currentMaxTimestamp.getMillis() - maxOutOfOrderness));
        ControlMessage controlMessage = ControlMessage.of(Type.WATERMARK, watermark);
        context.forward(null, controlMessage);
      }
    }
  }
}
