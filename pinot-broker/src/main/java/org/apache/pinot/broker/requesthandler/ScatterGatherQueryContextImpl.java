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

import static java.util.Objects.requireNonNull;

/**
 * Immutable value holder for {@link ScatterGatherQueryContext}.
 */
public class ScatterGatherQueryContextImpl implements ScatterGatherQueryContext {

  private final BrokerRequest _serverBrokerRequest;
  @Nullable
  private final Schema _schema;
  private final TableRouteInfo _route;
  private final RequestContext _requestContext;

  public ScatterGatherQueryContextImpl(BrokerRequest serverBrokerRequest, @Nullable Schema schema,
      TableRouteInfo route, RequestContext requestContext) {
    _serverBrokerRequest = requireNonNull(serverBrokerRequest);
    _schema = schema;
    _route = requireNonNull(route);
    _requestContext = requireNonNull(requestContext);
  }

  @Override
  public BrokerRequest serverBrokerRequest() {
    return _serverBrokerRequest;
  }

  @Override
  @Nullable
  public Schema schema() {
    return _schema;
  }

  @Override
  public TableRouteInfo route() {
    return _route;
  }

  @Override
  public RequestContext requestContext() {
    return _requestContext;
  }
}
