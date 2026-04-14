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
import org.apache.pinot.core.transport.ServerRoutingInstance;

/**
 * No-op implementation of {@link BrokerQueryPlugin}. Used when no plugin is configured.
 * {@link #preScatterGather} passes the context through unchanged; {@link #postScatterGather}
 * always returns empty (live response used as-is).
 * Thread-safe: stateless singleton.
 */
public class NoOpBrokerQueryPlugin implements BrokerQueryPlugin {

  public static final NoOpBrokerQueryPlugin INSTANCE = new NoOpBrokerQueryPlugin();

  private static final PluginContext NOOP_PLUGIN_CONTEXT = new PluginContext() {
  };

  private static final class PassthroughPreResult implements PreResult {
    private final ScatterGatherQueryContext _context;

    PassthroughPreResult(ScatterGatherQueryContext context) {
      _context = context;
    }

    @Override
    public ScatterGatherQueryContext originalContext() {
      return _context;
    }

    @Override
    public ScatterGatherQueryContext newContext() {
      return _context;
    }

    @Override
    public PluginContext pluginContext() {
      return NOOP_PLUGIN_CONTEXT;
    }
  }

  @Override
  public PreResult preScatterGather(ScatterGatherQueryContext context) {
    return new PassthroughPreResult(context);
  }

  @Override
  public Map<ServerRoutingInstance, DataTable> postScatterGather(PreResult preResult,
      Map<ServerRoutingInstance, DataTable> liveResponses) {
    return liveResponses;
  }
}
