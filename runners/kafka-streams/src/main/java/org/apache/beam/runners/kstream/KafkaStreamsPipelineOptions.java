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

import java.util.Properties;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options that configure the Kafka Streams pipeline.
 */
public interface KafkaStreamsPipelineOptions extends PipelineOptions {

  @Description("Application Name for Kafka Streams runner")
  String getApplicationName();

  void setApplicationName(String name);

  @Description("The config for Kafka Streams runner")
  Properties getStreamsConfiguration();

  void setStreamsConfiguration(Properties streamsConfiguration);

  @Description("The interval to check for watermarks in milliseconds")
  @Default.Long(1000)
  long getWatermarkInterval();

  void setWatermarkInterval(long interval);

  @Description("The number of partitions for topics created for sources")
  @Default.Integer(10)
  int getNumPartitions();

  void setNumPartitions(int numPartitions);

  @Description("The replication factor for topics created for sources")
  @Default.Short(1)
  short getReplicationFactor();

  void setReplicationFactor(short replicationFactor);

  @Description("The maximum parallelism allowed for a given data source")
  @Default.Integer(1)
  int getMaxSourceParallelism();

  void setMaxSourceParallelism(int maxSourceParallelism);
}

