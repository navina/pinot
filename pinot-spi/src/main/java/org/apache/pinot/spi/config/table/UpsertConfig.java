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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.spi.config.BaseJsonConfig;


/** Class representing upsert configuration of a table. */
public class UpsertConfig extends BaseJsonConfig {

  public enum Mode {
    FULL, PARTIAL, NONE
  }

  public enum Strategy {
    // Todo: add CUSTOM strategies
    APPEND, IGNORE, INCREMENT, MAX, MIN, OVERWRITE, UNION
  }

  @JsonPropertyDescription("Upsert mode.")
  private Mode _mode;

  @JsonPropertyDescription("Function to hash the primary key.")
  private HashFunction _hashFunction = HashFunction.NONE;

  /**
   * PartialUpsertStrategies maintains the mapping of merge strategies per column.
   * Each key in the map is a columnName, value is a partial upsert merging strategy.
   * Supported strategies are {OVERWRITE|INCREMENT|APPEND|UNION|IGNORE}.
   */
  @JsonPropertyDescription("Partial update strategies.")
  private Map<String, Strategy> _partialUpsertStrategies;

  /**
   * If strategy is not specified for a column, the merger on that column will be "defaultPartialUpsertStrategy".
   * The default value of defaultPartialUpsertStrategy is OVERWRITE.
   */
  @JsonPropertyDescription("default upsert strategy for partial mode")
  private Strategy _defaultPartialUpsertStrategy = Strategy.OVERWRITE;

  /**
   * By default, Pinot uses the value in the time column to determine the latest record. For two records with the
   * same primary key, the record with the larger value of the time column is picked as the
   * latest update.
   * However, there are cases when users need to use another column to determine the order.
   * In such case, you can use option comparisonColumn to override the column used for comparison. When using
   * multiple comparison columns, typically in the case of partial upserts, it is expected that input documents will
   * each only have a singular non-null comparisonColumn. Multiple non-null values in an input document _will_ result
   * in undefined behaviour. Typically, one comparisonColumn is allocated per distinct producer application of data
   * in the case where there are multiple producers sinking to the same table.
   */
  @JsonPropertyDescription("Columns for upsert comparison, default to time column")
  private List<String> _comparisonColumns;

  @JsonPropertyDescription("Whether to use snapshot for fast upsert metadata recovery")
  private boolean _enableSnapshot;

  @JsonPropertyDescription("Custom class for upsert metadata manager")
  private String _metadataManagerClass;

  @JsonPropertyDescription("Custom configs for upsert metadata manager")
  private Map<String, String> _metadataManagerConfigs;

  public UpsertConfig(Mode mode) {
    _mode = mode;
  }

  // Do not use this constructor. This is needed for JSON deserialization.
  public UpsertConfig() {
  }

  private UpsertConfig(Mode mode, HashFunction hashFunction, List<String> comparisonColumns,
      Map<String, Strategy> partialUpsertStrategies, Strategy defaultPartialUpsertStrategy, boolean enableSnapshot,
      String metadataManagerClass, Map<String, String> metadataManagerConfigs) {
    Preconditions.checkNotNull(mode, "Upsert `mode` cannot be null. Allowed values are : ",
        Arrays.asList(Mode.values()).stream().map(Enum::toString).collect(Collectors.joining(",")));
    _mode = mode;
    if (hashFunction != null) {
      _hashFunction = hashFunction;
    }
    if (CollectionUtils.isNotEmpty(comparisonColumns)) {
      _comparisonColumns = comparisonColumns;
    }
    _partialUpsertStrategies = partialUpsertStrategies;
    _defaultPartialUpsertStrategy = defaultPartialUpsertStrategy;
    _enableSnapshot = enableSnapshot;
    _metadataManagerClass = metadataManagerClass;
    if (metadataManagerConfigs != null) {
      _metadataManagerConfigs = metadataManagerConfigs;
    }
  }

  public static UpsertConfigBuilder newBuilder() {
    return new UpsertConfigBuilder();
  }

  public Mode getMode() {
    return _mode;
  }

  public HashFunction getHashFunction() {
    return _hashFunction;
  }

  @Nullable
  public Map<String, Strategy> getPartialUpsertStrategies() {
    return _partialUpsertStrategies;
  }

  public Strategy getDefaultPartialUpsertStrategy() {
    return _defaultPartialUpsertStrategy;
  }

  public List<String> getComparisonColumns() {
    return _comparisonColumns;
  }

  public boolean isEnableSnapshot() {
    return _enableSnapshot;
  }

  @Nullable
  public String getMetadataManagerClass() {
    return _metadataManagerClass;
  }

  @Nullable
  public Map<String, String> getMetadataManagerConfigs() {
    return _metadataManagerConfigs;
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class UpsertConfigBuilder {
    private Mode _mode;
    private List<String> _comparisonColumns;
    private HashFunction _hashFunction;
    private Map<String, Strategy> _partialUpsertStrategies = new HashMap<>();
    private boolean _enableSnapshot;
    private String _metadataManagerClass;
    private Map<String, String> _metadataManagerConfigs = new HashMap<>();
    private Strategy _defaultPrtialUpsertStrategy = Strategy.OVERWRITE;

    public UpsertConfigBuilder() {
    }

    public UpsertConfigBuilder mode(Mode mode) {
      _mode = mode;
      return this;
    }

    public UpsertConfigBuilder comparisonColumn(String comparisonColumn) {
      _comparisonColumns = Collections.singletonList(comparisonColumn);
      return this;
    }
    public UpsertConfigBuilder comparisonColumns(List<String> comparisonColumns) {
      _comparisonColumns = comparisonColumns;
      return this;
    }

    public UpsertConfigBuilder hashFunction(HashFunction hashFunction) {
      _hashFunction = hashFunction;
      return this;
    }

    public UpsertConfigBuilder partialUpsertStrategies(Map<String, Strategy> partialUpsertStrategies) {
      _partialUpsertStrategies = partialUpsertStrategies;
      return this;
    }

    public UpsertConfigBuilder defaultPartialUpsertStrategy(Strategy strategy) {
      _defaultPrtialUpsertStrategy = strategy;
      return this;
    }

    public UpsertConfigBuilder enableSnapshot(boolean enable) {
      _enableSnapshot = enable;
      return this;
    }

    public UpsertConfigBuilder metadataManagerClass(String metadataManagerClass) {
      _metadataManagerClass = metadataManagerClass;
      return this;
    }

    public UpsertConfigBuilder metadataManagerConfigs(Map<String, String> metadataManagerConfigs) {
      _metadataManagerConfigs = metadataManagerConfigs;
      return this;
    }

    public UpsertConfig build() {
      return new UpsertConfig(_mode, _hashFunction, _comparisonColumns, _partialUpsertStrategies,
          _defaultPrtialUpsertStrategy, _enableSnapshot, _metadataManagerClass, _metadataManagerConfigs);
    }
  }
}
