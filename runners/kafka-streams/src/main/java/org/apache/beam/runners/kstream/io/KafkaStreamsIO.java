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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.PunctuationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams IO.
 */
public class KafkaStreamsIO {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsIO.class);

  public static <K, V> Read<K, V> read() {
    return new AutoValue_KafkaStreamsIO_Read.Builder<K, V>()
        .setTopics(new ArrayList<>())
        .setWatermarkGenerationInterval(1000)
        .setWatermarkGenerationType(PunctuationType.WALL_CLOCK_TIME)
        .setMaxNumRecords(Long.MAX_VALUE)
        .build();
  }

  public static <K, V> Write<K, V> write() {
    return new AutoValue_KafkaStreamsIO_Write.Builder<K, V>()
        .build();
  }

  /**
   * A {@link PTransform} to read from a Kafka Stream.
   */
  @AutoValue
  public abstract static class Read<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {

    public abstract Collection<String> getTopics();

    @Nullable
    public abstract Coder<K> getKeyCoder();

    @Nullable
    public abstract Coder<V> getValueCoder();

    @Nullable
    public abstract Serde<K> getKeySerde();

    @Nullable
    public abstract Serde<V> getValueSerde();

    public abstract long getWatermarkGenerationInterval();

    @Nullable
    public abstract PunctuationType getWatermarkGenerationType();

    public abstract long getMaxNumRecords();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setTopics(Collection<String> topics);

      abstract Builder<K, V> setKeyCoder(Coder<K> keyCoder);

      abstract Builder<K, V> setValueCoder(Coder<V> valueCoder);

      abstract Builder<K, V> setKeySerde(Serde<K> keySerde);

      abstract Builder<K, V> setValueSerde(Serde<V> valueSerde);

      abstract Builder<K, V> setWatermarkGenerationInterval(long interval);

      abstract Builder<K, V> setWatermarkGenerationType(PunctuationType type);

      abstract Builder<K, V> setMaxNumRecords(long maxNumRecords);

      abstract Read<K, V> build();
    }

    /**
     * Sets a list of topics to read from. All the partitions from each
     * of the topics are read.
     */
    public Read<K, V> withTopics(Collection<String> topics) {
      return toBuilder().setTopics(ImmutableList.copyOf(topics)).build();
    }

    /**
     * Sets a Kafka {@link Serde} to interpret key bytes read from Kafka.
     * <p>
     * <p>In addition, Beam also needs a {@link Coder} to serialize and deserialize key objects at
     * runtime. KafkaIO tries to infer a coder for the key based on the {@link Serde} class,
     * however in case that fails, you can use {@link #withKeySerdeAndCoder(Serde, Coder)} to
     * provide the key coder explicitly.
     */
    public Read<K, V> withKeySerde(Serde<K> keySerde) {
      return toBuilder().setKeySerde(keySerde).build();
    }

    /**
     * Sets a Kafka {@link Serde} for interpreting key bytes read from Kafka along with a
     * {@link Coder} for helping the Beam runner materialize key objects at runtime if necessary.
     * <p>
     * <p>Use this method only if your pipeline doesn't work with plain {@link
     * #withKeySerde(Serde)}.
     */
    public Read<K, V> withKeySerdeAndCoder(Serde<K> keySerde, Coder<K> keyCoder) {
      return toBuilder().setKeySerde(keySerde).setKeyCoder(keyCoder).build();
    }

    /**
     * Sets a Kafka {@link Serde} to interpret value bytes read from Kafka.
     * <p>
     * <p>In addition, Beam also needs a {@link Coder} to serialize and deserialize value objects at
     * runtime. KafkaIO tries to infer a coder for the value based on the {@link Serde}
     * class, however in case that fails, you can use {@link #withValueSerdeAndCoder(Serde,
     * Coder)} to provide the value coder explicitly.
     */
    public Read<K, V> withValueSerde(Serde<V> valueSerde) {
      return toBuilder().setValueSerde(valueSerde).build();
    }

    /**
     * Sets a Kafka {@link Serde} for interpreting value bytes read from Kafka along with a
     * {@link Coder} for helping the Beam runner materialize value objects at runtime if necessary.
     * <p>
     * <p>Use this method only if your pipeline doesn't work with plain {@link
     * #withValueSerde(Serde)}.
     */
    public Read<K, V> withValueSerdeAndCoder(Serde<V> valueSerde, Coder<V> valueCoder) {
      return toBuilder().setValueSerde(valueSerde).setValueCoder(valueCoder).build();
    }

    @Override
    public PCollection<KV<K, V>> expand(PBegin input) {
      checkArgument(getTopics().size() > 0, "withTopic() is required");
      checkArgument(getKeySerde() != null, "withKeySerde() is required");
      checkArgument(getValueSerde() != null, "withValueSerde() is required");

      // Infer key/value coders if not specified explicitly
      CoderRegistry registry = input.getPipeline().getCoderRegistry();

      Coder<K> keyCoder =
          getKeyCoder() != null ? getKeyCoder() : inferCoder(registry, getKeySerde());
      checkArgument(
          keyCoder != null,
          "Key coder could not be inferred from key serde. Please provide"
              + "key coder explicitly using withKeySerdeAndCoder()"
      );

      Coder<V> valueCoder =
          getValueCoder() != null ? getValueCoder() : inferCoder(registry, getValueSerde());
      checkArgument(
          valueCoder != null,
          "Value coder could not be inferred from value serde. Please provide"
              + "value coder explicitly using withValueSerdeAndCoder()"
      );

      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          PCollection.IsBounded.UNBOUNDED,
          KvCoder.of(keyCoder, valueCoder)
      );
    }

    /**
     * Attempt to infer a {@link Coder} by extracting the type of the deserialized-class from the
     * serde argument using the {@link Coder} registry.
     */
    @VisibleForTesting
    static <T> NullableCoder<T> inferCoder(CoderRegistry coderRegistry, Serde<T> serde) {
      checkNotNull(serde);

      for (Type type : serde.getClass().getGenericInterfaces()) {
        if (!(type instanceof ParameterizedType)) {
          continue;
        }

        // This does not recurse: we will not infer from a class that extends
        // a class that extends Serde<T>.
        ParameterizedType parameterizedType = (ParameterizedType) type;

        if (parameterizedType.getRawType() == Serde.class) {
          Type parameter = parameterizedType.getActualTypeArguments()[0];

          @SuppressWarnings("unchecked")
          Class<T> clazz = (Class<T>) parameter;

          try {
            return NullableCoder.of(coderRegistry.getCoder(clazz));
          } catch (CannotProvideCoderException e) {
            throw new RuntimeException(
                String.format("Unable to automatically infer a Coder for "
                        + "the Kafka Serde %s: no coder registered for type %s",
                    serde, clazz
                ));
          }
        }
      }

      throw new RuntimeException(String.format(
          "Could not extract the Kafka Serde type from %s", serde));
    }
  }

  /**
   * A {@link PTransform} to write to a Kafka topic. See {@link KafkaStreamsIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Write<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {
    @Nullable
    public abstract String getTopic();

    @Nullable
    public abstract Serde<K> getKeySerde();

    @Nullable
    public abstract Serde<V> getValueSerde();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setTopic(String topic);

      abstract Builder<K, V> setKeySerde(Serde<K> keySerde);

      abstract Builder<K, V> setValueSerde(Serde<V> valueSerde);

      abstract Write<K, V> build();
    }

    /**
     * Sets the Kafka topic to write to.
     */
    public Write<K, V> withTopic(String topic) {
      return toBuilder().setTopic(topic).build();
    }

    /**
     * Sets a Kafka {@link Serde} to interpret key bytes read from Kafka.
     * <p>
     * <p>In addition, Beam also needs a {@link Coder} to serialize and deserialize key objects at
     * runtime.
     */
    public Write<K, V> withKeySerde(Serde<K> keySerde) {
      return toBuilder().setKeySerde(keySerde).build();
    }

    /**
     * Sets a Kafka {@link Serde} to interpret value bytes read from Kafka.
     * <p>
     * <p>In addition, Beam also needs a {@link Coder} to serialize and deserialize value objects at
     * runtime.
     */
    public Write<K, V> withValueSerde(Serde<V> valueSerde) {
      return toBuilder().setValueSerde(valueSerde).build();
    }

    @Override
    public PDone expand(PCollection<KV<K, V>> input) {
      checkArgument(getTopic() != null, "withTopic() is required");
      checkArgument(getKeySerde() != null, "withKeySerde() is required");
      checkArgument(getValueSerde() != null, "withValueSerde() is required");

      return PDone.in(input.getPipeline());
    }
  }


  private KafkaStreamsIO() {
  }

}
