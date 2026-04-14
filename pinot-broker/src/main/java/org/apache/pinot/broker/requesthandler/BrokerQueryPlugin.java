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

import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.env.PinotConfiguration;

/**
 * Extension point for intercepting the broker scatter-gather pipeline in the Single-Stage Engine.
 *
 * <p>A plugin is called twice per eligible query:</p>
 * <ol>
 *   <li>{@link #preScatterGather} — after routing and query optimization, before server dispatch.
 *       The plugin may rewrite the query context (e.g. narrow a time-range filter) by returning a
 *       modified {@link PreResult#newContext()}. State that must survive to
 *       {@link #postScatterGather} is stored in {@link PreResult#pluginContext()}.</li>
 *   <li>{@link #postScatterGather} — after scatter-gather completes, before reduce. The plugin
 *       receives the raw {@link DataTable} responses from servers and returns the set of DataTables
 *       the broker should reduce. The plugin may inject cached DataTables alongside live ones,
 *       or return {@code liveResponses} unchanged for a no-op.</li>
 * </ol>
 *
 * <p>Plugins are registered via the broker configuration key
 * {@link BrokerQueryPluginFactory#BROKER_QUERY_PLUGIN_CLASS_NAME} and must have a public no-arg
 * constructor. Implementations must be thread-safe.</p>
 *
 * <p>Example uses: result caching, circuit-breaking, query-level audit logging.</p>
 *
 * <p>Implementations that are opt-in per query should call {@link #isEnabled(ScatterGatherQueryContext)}
 * at the top of {@link #preScatterGather} and return a passthrough {@link PreResult} if false.</p>
 */
public interface BrokerQueryPlugin {

  /**
   * Called once when the broker starts. Plugins that hold resources (thread pools, caches,
   * network connections) should initialize them here.
   *
   * @param config the broker {@link PinotConfiguration}
   */
  default void init(PinotConfiguration config) {
  }

  /**
   * Called once when the broker shuts down. Plugins should release any resources acquired in
   * {@link #init}.
   */
  default void close() {
  }

  /**
   * Returns true if the {@code useBrokerResultCache} query option is set to {@code true} for this
   * request. Plugins that should only activate when explicitly requested by the caller should
   * guard {@link #preScatterGather} with this check.
   */
  default boolean isEnabled(ScatterGatherQueryContext context) {
    Map<String, String> queryOptions =
        context.serverBrokerRequest().getPinotQuery().getQueryOptions();
    return queryOptions != null && QueryOptionsUtils.isUseBrokerResultCache(queryOptions);
  }

  /**
   * Called before scatter-gather dispatch. The broker uses {@link PreResult#newContext()} for the
   * actual scatter-gather call; the plugin may return the original context unchanged (no-op) or a
   * modified copy.
   *
   * @param context the current query execution context
   * @return a {@link PreResult} carrying the (possibly rewritten) context and opaque plugin state
   */
  PreResult preScatterGather(ScatterGatherQueryContext context);

  /**
   * Called after scatter-gather completes, before reduce. The plugin returns the set of
   * {@link DataTable} responses the broker should pass to {@code reduceOnDataTable}. The plugin
   * may inject cached DataTables (keyed by the original {@link ServerRoutingInstance}) alongside
   * live ones, or return {@code liveResponses} unchanged for a no-op.
   *
   * @param preResult     the result returned by {@link #preScatterGather} for this request
   * @param liveResponses raw DataTable responses collected from servers, pre-reduce
   * @return the DataTables to reduce — may include cached entries alongside live ones
   */
  Map<ServerRoutingInstance, DataTable> postScatterGather(PreResult preResult,
      Map<ServerRoutingInstance, DataTable> liveResponses);
}
