package org.apache.pinot.core.data.manager.realtime;

import java.time.Clock;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
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
  private static final List<SegmentErrorInfo> EMPTY_ERROR_INFO = Collections.emptyList();
  private final TransformPipeline.Result reusedResult = new TransformPipeline.Result();

  public enum ERROR_CODE {
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
  private int _indexedMessageCount = 0;

  MessageProcessor(StreamDataDecoder dataDecoder, TransformPipeline transformPipeline, MutableSegment mutableSegment,
      ProcessingContext context, ServerMetrics serverMetrics) {
    this(dataDecoder, transformPipeline, mutableSegment, context, serverMetrics, Clock.systemUTC());
  }

  MessageProcessor(StreamDataDecoder dataDecoder, TransformPipeline transformPipeline, MutableSegment mutableSegment,
      ProcessingContext context, ServerMetrics serverMetrics, Clock clock) {
    _dataDecoder = dataDecoder;
    _transformPipeline = transformPipeline;
    _mutableSegment = mutableSegment;
    _context = context;
    _segmentLogger = LoggerFactory.getLogger(LLRealtimeSegmentDataManager.class.getName() + "_" +
        _context.getSegmentNameStr());
    _serverMetrics = serverMetrics;
    _clock = clock;
  }

  void resetBatchContext() {
    _realtimeRowsDroppedMeter = null;
    _realtimeRowsConsumedMeter = null;
    _realtimeIncompleteRowsConsumedMeter = null;
    _indexedMessageCount = 0;
  }

  // metadata is needed here for legacy reasons
  MessageProcessorResult process(StreamMessage message, RowMetadata metadata) {
    StreamDataDecoderResult decodedRow = _dataDecoder.decode(message);
    if (decodedRow.getException() != null) {
      // TODO: based on a config, decide whether the record should be silently dropped or stop further consumption on
      // decode error
      return failure(ERROR_CODE.DECODE_FAILURE, EMPTY_ERROR_INFO, -1);
    } else {
      try {
        _transformPipeline.processRow(decodedRow.getResult(), reusedResult);
        if (reusedResult.getSkippedRowCount() > 0) {
          _realtimeRowsDroppedMeter =
              _serverMetrics.addMeteredTableValue(_context.getPartitionLevelMetricKey(), ServerMeter.REALTIME_ROWS_FILTERED,
                  reusedResult.getSkippedRowCount(), _realtimeRowsDroppedMeter);
        }
        if (reusedResult.getIncompleteRowCount() > 0) {
          _realtimeIncompleteRowsConsumedMeter =
              _serverMetrics.addMeteredTableValue(_context.getPartitionLevelMetricKey(), ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED,
                  reusedResult.getIncompleteRowCount(), _realtimeIncompleteRowsConsumedMeter);
        }
      } catch (Exception e) {
        // when exception happens we prefer abandoning the whole batch and not partially indexing some rows
        reusedResult.getTransformedRows().clear();
        String errorMessage = String.format("Caught exception while transforming the record: %s", decodedRow);
        _segmentLogger.error(errorMessage, e);
        return failure(ERROR_CODE.TRANSFORM_FAILURE,
            Collections.singletonList(new SegmentErrorInfo(_clock.millis(), errorMessage, e)), -1);
      }

      List<SegmentErrorInfo> segmentErrors = new LinkedList<>();
      boolean canTakeMore = true;
      long lastIndexedTimestampMs = -1;
      for (GenericRow transformedRow : reusedResult.getTransformedRows()) {
        try {
          canTakeMore = _mutableSegment.index(transformedRow, metadata);
          _realtimeRowsConsumedMeter =
              _serverMetrics.addMeteredTableValue(_context.getPartitionLevelMetricKey(), ServerMeter.REALTIME_ROWS_CONSUMED, 1,
                  _realtimeRowsConsumedMeter);
          lastIndexedTimestampMs = _clock.millis();
          _indexedMessageCount++;
        } catch (Exception e) {
          String errorMessage = String.format("Caught exception while indexing the record: %s", transformedRow);
          _segmentLogger.error(errorMessage, e);
          segmentErrors.add(new SegmentErrorInfo(_clock.millis(), errorMessage, e));
        }
      }
      if (segmentErrors.size() > 0) {
        return failure(ERROR_CODE.INDEX_FAILURE, segmentErrors, lastIndexedTimestampMs);
      }
      return success(canTakeMore, lastIndexedTimestampMs);
    }
  }

  public MessageProcessorResult success(boolean canTakeMore, long lastIndexedTimestampMs) {
    return new MessageProcessorResult(ERROR_CODE.NONE, EMPTY_ERROR_INFO, canTakeMore, lastIndexedTimestampMs,
        _indexedMessageCount);
  }

  public MessageProcessorResult failure(ERROR_CODE errorCode, List<SegmentErrorInfo> errorInfoList,
      long lastIndexedTimestampMs) {
    return new MessageProcessorResult(errorCode, errorInfoList, false, lastIndexedTimestampMs,
        _indexedMessageCount);
  }

  class MessageProcessorResult {
    ERROR_CODE _errorCode;
    List<SegmentErrorInfo> _segmentErrorInfo;
    boolean _canTakeMore;
    long _lastIndexedTimestampMs;
    int _indexedMessageCount;

    MessageProcessorResult(ERROR_CODE errorCode, List<SegmentErrorInfo> segmentErrorInfo,
        boolean canTakeMore, long lastIndexedTimestampMs, int indexedMessageCount) {
      _errorCode = errorCode;
      _segmentErrorInfo = segmentErrorInfo;
      _canTakeMore = canTakeMore;
      _lastIndexedTimestampMs = lastIndexedTimestampMs;
      _indexedMessageCount = indexedMessageCount;
    }
  }

  class ProcessingContext {
    private final String _segmentNameStr;
    private final String _tableLevelMetricKey;
    private final String _partitionLevelMetricKey;

    ProcessingContext(String segmentNameStr, String tableLevelMetricKey, String partitionLevelMetricKey) {
      _segmentNameStr = segmentNameStr;
      _tableLevelMetricKey = tableLevelMetricKey;
      _partitionLevelMetricKey = partitionLevelMetricKey;
    }

    String getSegmentNameStr() {
      return _segmentNameStr;
    }

    public String getTableLevelMetricKey() {
      return _tableLevelMetricKey;
    }

    public String getPartitionLevelMetricKey() {
      return _partitionLevelMetricKey;
    }
  }
}
