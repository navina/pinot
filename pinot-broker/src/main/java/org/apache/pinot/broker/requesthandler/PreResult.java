/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.requesthandler;

/**
 * Result returned by {@link BrokerQueryPlugin#preScatterGather}. Carries three things:
 * <ol>
 *   <li>{@link #originalContext()} — the unmodified query context as computed by the broker.
 *       Available in {@link BrokerQueryPlugin#postScatterGather} for comparison, logging, and
 *       cache key derivation.</li>
 *   <li>{@link #newContext()} — the context the broker uses for scatter-gather. The plugin may
 *       return the original unchanged (no-op) or a modified copy with a rewritten route or
 *       broker request.</li>
 *   <li>{@link #pluginContext()} — opaque per-request state created by the plugin. Passed back
 *       in {@link BrokerQueryPlugin#postScatterGather} without inspection by the broker.</li>
 * </ol>
 */
public interface PreResult {

  /** The original, unmodified query context. */
  ScatterGatherQueryContext originalContext();

  /**
   * The query context to use for scatter-gather. May differ from {@link #originalContext()} if
   * the plugin rewrote the route or server broker request.
   */
  ScatterGatherQueryContext newContext();

  /** Opaque plugin state to be passed to {@link BrokerQueryPlugin#postScatterGather}. */
  PluginContext pluginContext();
}
