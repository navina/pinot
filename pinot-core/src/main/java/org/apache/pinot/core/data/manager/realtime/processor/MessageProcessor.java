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
package org.apache.pinot.core.data.manager.realtime.processor;

import java.time.Clock;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.stream.StreamDataDecoder;
import org.apache.pinot.spi.stream.StreamDataDecoderResult;
import org.apache.pinot.spi.stream.StreamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MessageProcessor {
  public static final List<SegmentErrorInfo> EMPTY_ERROR_INFO = Collections.emptyList();
  private final TransformPipeline.Result _reusedResult = new TransformPipeline.Result();

  public enum ERROR {
    NONE, DECODE_FAILURE, TRANSFORM_FAILURE, INDEX_FAILURE
  }

  private final StreamDataDecoder _dataDecoder;
  private final TransformPipeline _transformPipeline;
  private final MutableSegment _mutableSegment;
  private final ProcessingContext _context;
  private final ServerMetrics _serverMetrics;

  private final Logger _segmentLogger;
  private final Clock _clock;

  // batch-level values
  private PinotMeter _realtimeRowsDroppedMeter = null;
  private PinotMeter _realtimeRowsConsumedMeter = null;
  private PinotMeter _realtimeIncompleteRowsConsumedMeter = null;
  private int _indexedMessageCountPerBatch = 0;

  public MessageProcessor(StreamDataDecoder dataDecoder, TransformPipeline transformPipeline,
      MutableSegment mutableSegment, ProcessingContext context, ServerMetrics serverMetrics) {
    this(dataDecoder, transformPipeline, mutableSegment, context, serverMetrics, Clock.systemUTC());
  }

  MessageProcessor(StreamDataDecoder dataDecoder, TransformPipeline transformPipeline,
      MutableSegment mutableSegment, ProcessingContext context, ServerMetrics serverMetrics, Clock clock) {
    _dataDecoder = dataDecoder;
    _transformPipeline = transformPipeline;
    _mutableSegment = mutableSegment;
    _context = context;
    _segmentLogger = LoggerFactory.getLogger(LLRealtimeSegmentDataManager.class.getName() + "_"
        + _context.getSegmentNameStr());
    _serverMetrics = serverMetrics;
    _clock = clock;
  }

  public void resetBatchContext() {
    _realtimeRowsDroppedMeter = null;
    _realtimeRowsConsumedMeter = null;
    _realtimeIncompleteRowsConsumedMeter = null;
    _indexedMessageCountPerBatch = 0;
  }

  // metadata is needed here for legacy reasons
  public MessageProcessorResult process(StreamMessage message, RowMetadata metadata) {
    StreamDataDecoderResult decodedRow = _dataDecoder.decode(message);
    if (decodedRow.getException() != null) {
      // TODO: based on a config, decide whether the record should be silently dropped or stop further consumption on
      // decode error
      return MessageProcessorResult.failure(ERROR.DECODE_FAILURE, EMPTY_ERROR_INFO, -1,
          _indexedMessageCountPerBatch);
    } else {
      try {
        _transformPipeline.processRow(decodedRow.getResult(), _reusedResult);
        if (_reusedResult.getSkippedRowCount() > 0) {
          _realtimeRowsDroppedMeter = _serverMetrics.addMeteredTableValue(_context.getPartitionLevelMetricKey(),
              ServerMeter.REALTIME_ROWS_FILTERED, _reusedResult.getSkippedRowCount(), _realtimeRowsDroppedMeter);
        }
        if (_reusedResult.getIncompleteRowCount() > 0) {
          _realtimeIncompleteRowsConsumedMeter = _serverMetrics.addMeteredTableValue(
              _context.getPartitionLevelMetricKey(), ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED,
              _reusedResult.getIncompleteRowCount(), _realtimeIncompleteRowsConsumedMeter);
        }
      } catch (Exception e) {
        // when exception happens we prefer abandoning the whole batch and not partially indexing some rows
        _reusedResult.getTransformedRows().clear();
        String errorMessage = String.format("Caught exception while transforming the record: %s", decodedRow);
        _segmentLogger.error(errorMessage, e);
        return MessageProcessorResult.failure(ERROR.TRANSFORM_FAILURE,
            Collections.singletonList(new SegmentErrorInfo(_clock.millis(), errorMessage, e)), -1,
            _indexedMessageCountPerBatch);
      }

      List<SegmentErrorInfo> segmentErrors = new LinkedList<>();
      boolean canTakeMore = true;
      long lastIndexedTimestampMs = -1;
      for (GenericRow transformedRow : _reusedResult.getTransformedRows()) {
        try {
          canTakeMore = _mutableSegment.index(transformedRow, metadata);
          _realtimeRowsConsumedMeter = _serverMetrics.addMeteredTableValue(_context.getPartitionLevelMetricKey(),
              ServerMeter.REALTIME_ROWS_CONSUMED, 1, _realtimeRowsConsumedMeter);
          lastIndexedTimestampMs = _clock.millis();
          _indexedMessageCountPerBatch++;
        } catch (Exception e) {
          String errorMessage = String.format("Caught exception while indexing the record: %s", transformedRow);
          _segmentLogger.error(errorMessage, e);
          segmentErrors.add(new SegmentErrorInfo(_clock.millis(), errorMessage, e));
        }
      }
      if (segmentErrors.size() > 0) {
        return MessageProcessorResult.failure(ERROR.INDEX_FAILURE, segmentErrors, lastIndexedTimestampMs,
            _indexedMessageCountPerBatch);
      }
      return MessageProcessorResult.success(canTakeMore, lastIndexedTimestampMs, _indexedMessageCountPerBatch);
    }
  }
}
