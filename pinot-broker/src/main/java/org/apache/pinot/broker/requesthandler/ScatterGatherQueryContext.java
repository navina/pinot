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

import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.trace.RequestContext;

/**
 * Carries the physical execution state of a query at the scatter-gather boundary: the server-side
 * broker request, resolved schema, routing information, and request context.
 *
 * <p>Both the original (unmodified) and rewritten (plugin-modified) states are represented by
 * this type. A {@link BrokerQueryPlugin} receives the original context in
 * {@link BrokerQueryPlugin#preScatterGather} and may return a modified copy as
 * {@link PreResult#newContext()} to change what is dispatched to servers.</p>
 *
 * <p>Not to be confused with {@code org.apache.pinot.core.query.request.context.QueryContext},
 * which represents the parsed logical query.</p>
 */
public interface ScatterGatherQueryContext {

  /** The broker request as prepared for server dispatch. May be rewritten by a plugin. */
  BrokerRequest serverBrokerRequest();

  /** The table schema, or {@code null} if not resolved (e.g. unknown table). */
  @Nullable
  Schema schema();

  /** Routing information including offline/realtime split and server assignments. */
  TableRouteInfo route();

  /** Per-request tracing and metadata context. */
  RequestContext requestContext();
}
