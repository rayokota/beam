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
package org.apache.beam.runners.fnexecution.control;

import com.google.auto.value.AutoValue;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterResponse;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;

/**
 * A high-level client for an SDK harness.
 *
 * <p>This provides a Java-friendly wrapper around {@link FnApiControlClient} and {@link
 * CloseableFnDataReceiver}, which handle lower-level gRPC message wrangling.
 */
public class SdkHarnessClient {

  /**
   * A supply of unique identifiers, used internally. These must be unique across all Fn API
   * clients.
   */
  public interface IdGenerator {
    String getId();
  }

  /** A supply of unique identifiers that are simply incrementing longs. */
  private static class CountingIdGenerator implements IdGenerator {
    private final AtomicLong nextId = new AtomicLong(0L);

    @Override
    public String getId() {
      return String.valueOf(nextId.incrementAndGet());
    }
  }


  /**
   * A processor capable of creating bundles for some registered {@link ProcessBundleDescriptor}.
   */
  public class BundleProcessor {
    private final String processBundleDescriptorId;
    private final Future<RegisterResponse> registrationFuture;

    private BundleProcessor(
        String processBundleDescriptorId, Future<RegisterResponse> registrationFuture) {
      this.processBundleDescriptorId = processBundleDescriptorId;
      this.registrationFuture = registrationFuture;
    }

    public Future<RegisterResponse> getRegistrationFuture() {
      return registrationFuture;
    }

    /**
     * Start a new bundle for the given {@link BeamFnApi.ProcessBundleDescriptor} identifier.
     *
     * <p>The input channels for the returned {@link ActiveBundle} are derived from the instructions
     * in the {@link BeamFnApi.ProcessBundleDescriptor}.
     */
    public ActiveBundle newBundle() {
      String bundleId = idGenerator.getId();

      // TODO: acquire an input receiver from appropriate FnDataService
      CloseableFnDataReceiver dataReceiver =
          new CloseableFnDataReceiver() {
            @Override
            public void close() throws Exception {
              throw new UnsupportedOperationException(
                  String.format(
                      "Placeholder %s cannot be closed.",
                      CloseableFnDataReceiver.class.getSimpleName()));
            }

            @Override
            public void accept(Object input) throws Exception {
              throw new UnsupportedOperationException(
                  String.format(
                      "Placeholder %s cannot accept data.",
                      CloseableFnDataReceiver.class.getSimpleName()));
            }
          };

      final ListenableFuture<BeamFnApi.InstructionResponse> genericResponse =
          fnApiControlClient.handle(
              BeamFnApi.InstructionRequest.newBuilder()
                  .setInstructionId(bundleId)
                  .setProcessBundle(
                      BeamFnApi.ProcessBundleRequest.newBuilder()
                          .setProcessBundleDescriptorReference(processBundleDescriptorId))
                  .build());

      ListenableFuture<BeamFnApi.ProcessBundleResponse> specificResponse =
          Futures.transform(
              genericResponse,
              InstructionResponse::getProcessBundle,
              MoreExecutors.directExecutor());

      return ActiveBundle.create(bundleId, specificResponse, dataReceiver);
    }
  }

  /** An active bundle for a particular {@link BeamFnApi.ProcessBundleDescriptor}. */
  @AutoValue
  public abstract static class ActiveBundle<InputT> {
    public abstract String getBundleId();

    public abstract Future<BeamFnApi.ProcessBundleResponse> getBundleResponse();

    public abstract CloseableFnDataReceiver<InputT> getInputReceiver();

    public static <InputT> ActiveBundle<InputT> create(
        String bundleId,
        Future<BeamFnApi.ProcessBundleResponse> response,
        CloseableFnDataReceiver<InputT> dataReceiver) {
      return new AutoValue_SdkHarnessClient_ActiveBundle<>(bundleId, response, dataReceiver);
    }
  }

  private final IdGenerator idGenerator;
  private final FnApiControlClient fnApiControlClient;

  private final Cache<String, BundleProcessor> clientProcessors =
      CacheBuilder.newBuilder().build();

  private SdkHarnessClient(FnApiControlClient fnApiControlClient, IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
    this.fnApiControlClient = fnApiControlClient;
  }

  /**
   * Creates a client for a particular SDK harness. It is the responsibility of the caller to ensure
   * that these correspond to the same SDK harness, so control plane and data plane messages can be
   * correctly associated.
   */
  public static SdkHarnessClient usingFnApiClient(FnApiControlClient fnApiControlClient) {
    return new SdkHarnessClient(fnApiControlClient, new CountingIdGenerator());
  }

  public SdkHarnessClient withIdGenerator(IdGenerator idGenerator) {
    return new SdkHarnessClient(fnApiControlClient, idGenerator);
  }

  public BundleProcessor getProcessor(final BeamFnApi.ProcessBundleDescriptor descriptor) {
    try {
      return clientProcessors.get(
          descriptor.getId(),
              () -> register(Collections.singleton(descriptor)).get(descriptor.getId()));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Registers a {@link BeamFnApi.ProcessBundleDescriptor} for future processing.
   *
   * <p>A client may block on the result future, but may also proceed without blocking.
   */
  public Map<String, BundleProcessor> register(
      Iterable<BeamFnApi.ProcessBundleDescriptor> processBundleDescriptors) {

    // TODO: validate that all the necessary data endpoints are known
    ListenableFuture<BeamFnApi.InstructionResponse> genericResponse =
        fnApiControlClient.handle(
            BeamFnApi.InstructionRequest.newBuilder()
                .setInstructionId(idGenerator.getId())
                .setRegister(
                    BeamFnApi.RegisterRequest.newBuilder()
                        .addAllProcessBundleDescriptor(processBundleDescriptors)
                        .build())
                .build());

    ListenableFuture<RegisterResponse> registerResponseFuture =
        Futures.transform(
            genericResponse, InstructionResponse::getRegister,
            MoreExecutors.directExecutor());
    for (BeamFnApi.ProcessBundleDescriptor processBundleDescriptor : processBundleDescriptors) {
      clientProcessors.put(
          processBundleDescriptor.getId(),
          new BundleProcessor(processBundleDescriptor.getId(), registerResponseFuture));
    }

    return clientProcessors.asMap();
  }

  /**
   * A pair of {@link Coder} and {@link BeamFnApi.Target} which can be handled by the remote SDK
   * harness to receive elements sent from the runner.
   */
  @AutoValue
  public abstract static class RemoteInputDestination<T> {
    public static <T> RemoteInputDestination<T> of(Coder<T> coder, BeamFnApi.Target target) {
      return new AutoValue_SdkHarnessClient_RemoteInputDestination(coder, target);
    }

    public abstract Coder<T> getCoder();
    public abstract BeamFnApi.Target getTarget();
  }

  /**
   * A pair of {@link Coder} and {@link FnDataReceiver} which can be registered to receive elements
   * for a {@link LogicalEndpoint}.
   */
  @AutoValue
  public abstract static class RemoteOutputReceiver<T> {
    public static <T> RemoteOutputReceiver of (Coder<T> coder, FnDataReceiver<T> receiver) {
      return new AutoValue_SdkHarnessClient_RemoteOutputReceiver(coder, receiver);
    }

    public abstract Coder<T> getCoder();
    public abstract FnDataReceiver<T> getReceiver();
  }
}
