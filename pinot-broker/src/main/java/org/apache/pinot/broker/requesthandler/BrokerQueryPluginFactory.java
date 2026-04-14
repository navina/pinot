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

import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a {@link BrokerQueryPlugin} from broker configuration.
 *
 * <p>Configure via: {@code pinot.broker.query.plugin.class.name = com.example.MyPlugin}
 * The class must have a public no-arg constructor. If the config key is absent or empty,
 * {@link NoOpBrokerQueryPlugin} is used.</p>
 */
public class BrokerQueryPluginFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerQueryPluginFactory.class);

  public static final String BROKER_QUERY_PLUGIN_CLASS_NAME = "pinot.broker.query.plugin.class.name";

  private BrokerQueryPluginFactory() {
  }

  public static BrokerQueryPlugin create(PinotConfiguration config) {
    String className = config.getProperty(BROKER_QUERY_PLUGIN_CLASS_NAME);
    if (className == null || className.isBlank()) {
      return NoOpBrokerQueryPlugin.INSTANCE;
    }
    try {
      BrokerQueryPlugin plugin =
          (BrokerQueryPlugin) Class.forName(className).getDeclaredConstructor().newInstance();
      LOGGER.info("Loaded BrokerQueryPlugin: {}", className);
      return plugin;
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate BrokerQueryPlugin: " + className, e);
    }
  }
}
