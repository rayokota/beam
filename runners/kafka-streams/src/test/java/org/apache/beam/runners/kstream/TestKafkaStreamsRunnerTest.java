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
package org.apache.beam.runners.kstream;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFns;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;

public class TestKafkaStreamsRunnerTest {

  protected static Pipeline getPipeline() {
    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.as(KafkaStreamsPipelineOptions.class);
    options.setRunner(TestKafkaStreamsRunner.class);
    return Pipeline.create(options);
  }

  @Ignore
  @Test
  public void testComposedCombine() {
    Pipeline p = getPipeline();
    p.getCoderRegistry().registerCoderForClass(UserString.class, UserStringCoder.of());

    PCollection<KV<String, KV<Integer, UserString>>> perKeyInput = p.apply(
        Create.timestamped(
            Arrays.asList(
                KV.of("a", KV.of(1, UserString.of("1"))),
                KV.of("a", KV.of(1, UserString.of("1"))),
                KV.of("a", KV.of(4, UserString.of("4"))),
                KV.of("b", KV.of(1, UserString.of("1"))),
                KV.of("b", KV.of(13, UserString.of("13")))
            ),
            Arrays.asList(0L, 4L, 7L, 10L, 16L)
        )
            .withCoder(KvCoder.of(
                StringUtf8Coder.of(),
                KvCoder.of(BigEndianIntegerCoder.of(), UserStringCoder.of())
            )));

    TupleTag<Integer> maxIntTag = new TupleTag<>();
    TupleTag<UserString> concatStringTag = new TupleTag<>();
    PCollection<KV<String, KV<Integer, String>>> combineGlobally =
        perKeyInput
            .apply(Values.create())
            .apply(
                Combine.globally(
                    CombineFns.compose()
                        .with(new GetIntegerFunction(), Max.ofIntegers(), maxIntTag)
                        .with(new GetUserStringFunction(), new ConcatString(), concatStringTag)))
            .apply(WithKeys.of("global"))
            .apply(
                "ExtractGloballyResult",
                ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag))
            );

    PCollection<KV<String, KV<Integer, String>>> combinePerKey =
        perKeyInput
            .apply(
                Combine.perKey(
                    CombineFns.compose()
                        .with(new GetIntegerFunction(), Max.ofIntegers(), maxIntTag)
                        .with(new GetUserStringFunction(), new ConcatString(), concatStringTag)))
            .apply(
                "ExtractPerKeyResult", ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag)));
    PAssert.that(combineGlobally).containsInAnyOrder(
        KV.of("global", KV.of(13, "111134")));
    PAssert.that(combinePerKey).containsInAnyOrder(
        KV.of("a", KV.of(4, "114")),
        KV.of("b", KV.of(13, "113"))
    );
    p.run();
  }

  public static void addCountingAsserts(PCollection<Long> input, long numElements) {
    // Count == numElements
    PAssert.thatSingleton(input.apply("Count", Count.globally())).isEqualTo(numElements);
    // Unique count == numElements
    PAssert.thatSingleton(input.apply(Distinct.create()).apply("UniqueCount", Count.globally()))
        .isEqualTo(numElements);
    // Min == 0
    PAssert.thatSingleton(input.apply("Min", Min.globally())).isEqualTo(0L);
    // Max == numElements-1
    PAssert.thatSingleton(input.apply("Max", Max.globally())).isEqualTo(numElements - 1);
  }

  @Ignore
  @Test
  public void testUnboundedSource() {
    long numElements = 1000;

    Pipeline p = getPipeline();
    PCollection<Long> input = p
        .apply(Read.from(CountingSource.unbounded()).withMaxNumRecords(numElements));

    addCountingAsserts(input, numElements);
    p.run();
  }

  @Ignore
  @Test
  public void testUnboundedSourceTimestamps() {
    long numElements = 1000;

    Pipeline p = getPipeline();
    PCollection<Long> input = p.apply(
        Read.from(CountingSource.unboundedWithTimestampFn(new ValueAsTimestampFn()))
            .withMaxNumRecords(numElements));
    addCountingAsserts(input, numElements);

    PCollection<Long> diffs =
        input
            .apply("TimestampDiff", ParDo.of(new ElementValueDiff()))
            .apply("DistinctTimestamps", Distinct.create());
    // This assert also confirms that diffs only has one unique value.
    PAssert.thatSingleton(diffs).isEqualTo(0L);

    p.run();
  }

  private static class ElementValueDiff extends DoFn<Long, Long> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element() - c.timestamp().getMillis());
    }
  }

  private static class ValueAsTimestampFn implements SerializableFunction<Long, Instant> {
    @Override
    public Instant apply(Long input) {
      return new Instant(input);
    }
  }

  @Ignore
  @Test
  public void testParDoWithSideInputs() {
    Pipeline pipeline = getPipeline();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    PCollectionView<Integer> sideInput1 =
        pipeline
            .apply("CreateSideInput1", Create.of(11))
            .apply("ViewSideInput1", View.asSingleton());
    PCollectionView<Integer> sideInputUnread =
        pipeline
            .apply("CreateSideInputUnread", Create.of(-3333))
            .apply("ViewSideInputUnread", View.asSingleton());
    PCollectionView<Integer> sideInput2 =
        pipeline
            .apply("CreateSideInput2", Create.of(222))
            .apply("ViewSideInput2", View.asSingleton());

    PCollection<String> output =
        pipeline
            .apply(Create.of(inputs))
            .apply(
                ParDo.of(new TestDoFn(Arrays.asList(sideInput1, sideInput2), Arrays.asList()))
                    .withSideInputs(sideInput1, sideInputUnread, sideInput2));

    PAssert.that(output)
        .satisfies(HasExpectedOutput
            .forInput(inputs)
            .andSideInputs(11, 222));

    pipeline.run();
  }

  static class TestDoFn extends DoFn<Integer, String> {
    enum State {NOT_SET_UP, UNSTARTED, STARTED, PROCESSING, FINISHED}


    State state = State.NOT_SET_UP;

    final List<PCollectionView<Integer>> sideInputViews = new ArrayList<>();
    final List<TupleTag<String>> additionalOutputTupleTags = new ArrayList<>();

    public TestDoFn() {
    }

    public TestDoFn(
        List<PCollectionView<Integer>> sideInputViews,
        List<TupleTag<String>> additionalOutputTupleTags
    ) {
      this.sideInputViews.addAll(sideInputViews);
      this.additionalOutputTupleTags.addAll(additionalOutputTupleTags);
    }

    @Setup
    public void prepare() {
      assertEquals(State.NOT_SET_UP, state);
      state = State.UNSTARTED;
    }

    @StartBundle
    public void startBundle() {
      assertThat(
          state,
          anyOf(equalTo(State.UNSTARTED), equalTo(State.FINISHED))
      );

      state = State.STARTED;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      assertThat(
          state,
          anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING))
      );
      state = State.PROCESSING;
      outputToAllWithSideInputs(c, "processing: " + c.element());
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      assertThat(
          state,
          anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING))
      );
      state = State.FINISHED;
      c.output("finished", BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE);
      for (TupleTag<String> additionalOutputTupleTag : additionalOutputTupleTags) {
        c.output(
            additionalOutputTupleTag,
            additionalOutputTupleTag.getId() + ": " + "finished",
            BoundedWindow.TIMESTAMP_MIN_VALUE,
            GlobalWindow.INSTANCE
        );
      }
    }

    private void outputToAllWithSideInputs(ProcessContext c, String value) {
      if (!sideInputViews.isEmpty()) {
        List<Integer> sideInputValues = new ArrayList<>();
        for (PCollectionView<Integer> sideInputView : sideInputViews) {
          sideInputValues.add(c.sideInput(sideInputView));
        }
        value += ": " + sideInputValues;
      }
      c.output(value);
      for (TupleTag<String> additionalOutputTupleTag : additionalOutputTupleTags) {
        c.output(
            additionalOutputTupleTag,
            additionalOutputTupleTag.getId() + ": " + value
        );
      }
    }
  }

  @Ignore
  @Test
  public void testMultipleApply() {
    Pipeline pipeline = getPipeline();
    PTransform<PCollection<? extends String>, PCollection<String>> myTransform =
        addSuffix("+");

    PCollection<String> input = pipeline.apply(Create.of(ImmutableList.of("a", "b")));

    PCollection<String> left = input.apply("Left1", myTransform).apply("Left2", myTransform);
    PCollection<String> right = input.apply("Right", myTransform);

    PCollection<String> both = PCollectionList.of(left).and(right).apply(Flatten.pCollections());

    PAssert.that(both).containsInAnyOrder("a++", "b++", "a+", "b+");

    pipeline.run();
  }

  private static PTransform<PCollection<? extends String>, PCollection<String>> addSuffix(
      final String suffix
  ) {
    return MapElements.via(new SimpleFunction<String, String>() {
      @Override
      public String apply(String input) {
        return input + suffix;
      }
    });
  }

  @Ignore
  @Test
  public void testTupleProjectionTransform() throws Exception {
    Pipeline pipeline = getPipeline();
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4));

    TupleTag<Integer> tag = new TupleTag<>();
    PCollectionTuple tuple = PCollectionTuple.of(tag, input);

    PCollection<Integer> output = tuple.apply("ProjectTag", new TupleProjectionTransform<>(tag));

    PAssert.that(output).containsInAnyOrder(1, 2, 3, 4);
    pipeline.run();
  }

  private static class TupleProjectionTransform<T>
      extends PTransform<PCollectionTuple, PCollection<T>> {
    private TupleTag<T> tag;

    public TupleProjectionTransform(TupleTag<T> tag) {
      this.tag = tag;
    }

    @Override
    public PCollection<T> expand(PCollectionTuple input) {
      return input.get(tag);
    }
  }

  private static class UserString implements Serializable {
    private String strValue;

    static UserString of(String strValue) {
      UserString ret = new UserString();
      ret.strValue = strValue;
      return ret;
    }
  }

  private static class UserStringCoder extends AtomicCoder<UserString> {
    public static UserStringCoder of() {
      return INSTANCE;
    }

    private static final UserStringCoder INSTANCE = new UserStringCoder();

    @Override
    public void encode(UserString value, OutputStream outStream)
        throws CoderException, IOException {
      encode(value, outStream, Context.NESTED);
    }

    @Override
    public void encode(UserString value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      StringUtf8Coder.of().encode(value.strValue, outStream, context);
    }

    @Override
    public UserString decode(InputStream inStream) throws CoderException, IOException {
      return decode(inStream, Context.NESTED);
    }

    @Override
    public UserString decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      return UserString.of(StringUtf8Coder.of().decode(inStream, context));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
    }
  }

  private static class GetIntegerFunction
      extends SimpleFunction<KV<Integer, UserString>, Integer> {
    @Override
    public Integer apply(KV<Integer, UserString> input) {
      return input.getKey();
    }
  }

  private static class GetUserStringFunction
      extends SimpleFunction<KV<Integer, UserString>, UserString> {
    @Override
    public UserString apply(KV<Integer, UserString> input) {
      return input.getValue();
    }
  }

  private static class ConcatString extends Combine.BinaryCombineFn<UserString> {
    @Override
    public UserString apply(UserString left, UserString right) {
      String retStr = left.strValue + right.strValue;
      char[] chars = retStr.toCharArray();
      Arrays.sort(chars);
      return UserString.of(new String(chars));
    }
  }

  private static class OutputNullString extends Combine.BinaryCombineFn<UserString> {
    @Override
    public UserString apply(UserString left, UserString right) {
      return null;
    }
  }

  private static class ExtractResultDoFn
      extends DoFn<KV<String, CombineFns.CoCombineResult>, KV<String, KV<Integer, String>>> {

    private final TupleTag<Integer> maxIntTag;
    private final TupleTag<UserString> concatStringTag;

    ExtractResultDoFn(TupleTag<Integer> maxIntTag, TupleTag<UserString> concatStringTag) {
      this.maxIntTag = maxIntTag;
      this.concatStringTag = concatStringTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      UserString userString = c.element().getValue().get(concatStringTag);
      KV<Integer, String> value = KV.of(
          c.element().getValue().get(maxIntTag),
          userString == null ? null : userString.strValue
      );
      c.output(KV.of(c.element().getKey(), value));
    }
  }

  static class HasExpectedOutput
      implements SerializableFunction<Iterable<String>, Void>, Serializable {
    private final List<Integer> inputs;
    private final List<Integer> sideInputs;
    private final String additionalOutput;

    public static HasExpectedOutput forInput(List<Integer> inputs) {
      return new HasExpectedOutput(new ArrayList<>(inputs), new ArrayList<>(), "");
    }

    private HasExpectedOutput(
        List<Integer> inputs,
        List<Integer> sideInputs,
        String additionalOutput
    ) {
      this.inputs = inputs;
      this.sideInputs = sideInputs;
      this.additionalOutput = additionalOutput;
    }

    public HasExpectedOutput andSideInputs(Integer... sideInputValues) {
      return new HasExpectedOutput(
          inputs, Arrays.asList(sideInputValues), additionalOutput);
    }

    public HasExpectedOutput fromOutput(TupleTag<String> outputTag) {
      return fromOutput(outputTag.getId());
    }

    public HasExpectedOutput fromOutput(String outputId) {
      return new HasExpectedOutput(inputs, sideInputs, outputId);
    }

    @Override
    public Void apply(Iterable<String> outputs) {
      List<String> processeds = new ArrayList<>();
      List<String> finisheds = new ArrayList<>();
      for (String output : outputs) {
        if (output.contains("finished")) {
          finisheds.add(output);
        } else {
          processeds.add(output);
        }
      }

      String sideInputsSuffix;
      if (sideInputs.isEmpty()) {
        sideInputsSuffix = "";
      } else {
        sideInputsSuffix = ": " + sideInputs;
      }

      String additionalOutputPrefix;
      if (additionalOutput.isEmpty()) {
        additionalOutputPrefix = "";
      } else {
        additionalOutputPrefix = additionalOutput + ": ";
      }

      List<String> expectedProcesseds = new ArrayList<>();
      for (Integer input : inputs) {
        expectedProcesseds.add(
            additionalOutputPrefix + "processing: " + input + sideInputsSuffix);
      }
      String[] expectedProcessedsArray =
          expectedProcesseds.toArray(new String[expectedProcesseds.size()]);
      assertThat(processeds, containsInAnyOrder(expectedProcessedsArray));

      for (String finished : finisheds) {
        assertEquals(additionalOutputPrefix + "finished", finished);
      }

      return null;
    }
  }
}
