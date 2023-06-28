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
package org.apache.pinot.plugin.minion.tasks.querytosegments;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
import org.apache.pinot.plugin.minion.tasks.BaseTaskExecutor;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.RecordReaderSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationTaskSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.grpc.GrpcRequestBuilder;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.commons.lang.StringUtils;

/**
 * QueryToSegmentsTaskExecutor implements a minion task to build a single Pinot segment based on one input file
 * given task configs.
 *
 * Task configs:
 *   input.data.file.uri - Required, the location of input file
 *   inputFormat - Required, the input file format, e.g. JSON/Avro/Parquet/CSV/...
 *   input.fs.className - Optional, the class name of filesystem to read input data. Default to PinotLocalFs if not
 *   specified.
 *   input.fs.prop.<keys> - Optional, defines the configs to initialize input filesystem.
 *
 *   output.segment.dir.uri -  Optional, the location of generated segment. Use local temp dir with push mode TAR, If
 *   not specified.
 *   output.fs.className - Optional, the class name of filesystem to write output segment. Default to PinotLocalFs if
 *   not specified.
 *   output.fs.prop.<keys> - Optional, the configs to initialize output filesystem.
 *   overwriteOutput - Optional, delete the output segment directory if set to true.
 *
 *   recordReader.className - Required, the class name of RecordReader.
 *   recordReader.configClassName - Required, the class name of RecordReaderConfig.
 *   recordReader.prop.<keys> - Optional, the configs used to initialize RecordReaderConfig.
 *
 *   schema - Required, if schemaURI is not specified. Pinot schema in Json string.
 *   schemaURI - Required, if schema is not specified. The URI to query for Pinot schema.
 *
 *   sequenceId - Optional, an option to set segment name.
 *   segmentNameGenerator.type - Required, the segment name generator to create segment name.
 *   segmentNameGenerator.configs.<keys> - Optional, configs of segment name generator.
 *
 *   push.mode - Required, push job type: TAR/URI/METADATA
 *   push.controllerUri - Required, controller uri to send push request to.
 *   push.segmentUriPrefix - Optional, segment download uri prefix, used when push.mode=uri
 *   push.segmentUriSuffix - Optional, segment download uri suffix, used when push.mode=uri
 *
 */
public class QueryToSegmentsTaskExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryToSegmentsTaskExecutor.class);

  private static final int DEFUALT_PUSH_ATTEMPTS = 5;
  private static final int DEFAULT_PUSH_PARALLELISM = 1;
  private static final long DEFAULT_PUSH_RETRY_INTERVAL_MILLIS = 1000L;

  private PinotTaskConfig _pinotTaskConfig;
  private MinionEventObserver _eventObserver;

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object executeTask(PinotTaskConfig pinotTaskConfig)
      throws Exception {
    LOGGER.info("Executing QueryToSegmentsTask with task config: {}", pinotTaskConfig);
    Map<String, String> taskConfigs = pinotTaskConfig.getConfigs();
    QueryToSegmentsResult.Builder resultBuilder = new QueryToSegmentsResult.Builder();
    File localTempDir = new File(new File(MinionContext.getInstance().getDataDir(), "QueryToSegmentsResult"),
        "tmp-" + UUID.randomUUID());
    _pinotTaskConfig = pinotTaskConfig;
    _eventObserver = MinionEventObservers.getInstance().getMinionEventObserver(pinotTaskConfig.getTaskId());

    String tableNameWithType = taskConfigs.get(BatchConfigProperties.TABLE_NAME);
    String sql = taskConfigs.get(BatchConfigProperties.SQL);
    List<String> segments = Arrays.asList(taskConfigs.get(BatchConfigProperties.SEGMENTS).split(";"));

    GrpcRequestBuilder grpcRequestBuilder = new GrpcRequestBuilder()
      .setSegments(segments)
      .setEnableStreaming(true)
      .setBrokerId("query-to-segments-task-executor")
      // .addExtraMetadata(pinotConfig.getExtraGrpcMetadata())
      .setSql(sql);
    Map<String, Object> grpcConfig;
    GrpcQueryClient client = new GrpcQueryClient(host, port, new GrpcConfig(grpcConfig));
    Iterator<Server.ServerResponse> serverResponseIterator = client.submit(grpcRequestBuilder.build());

    ByteBuffer byteBuffer = null;
    int segmentCount = 0;
    try {
      // Pinot gRPC server response iterator returns:
      //   - n data blocks based on inbound message size;
      //   - 1 metadata of the query results.
      // So we need to check ResponseType of each ServerResponse.
      while (serverResponseIterator.hasNext()) {
        long startTimeNanos = System.nanoTime();
        Server.ServerResponse serverResponse = serverResponseIterator.next();
        final String responseType = serverResponse.getMetadataOrThrow("responseType");
        switch (responseType) {
            case CommonConstants.Query.Response.ResponseType.DATA:
              try {
                byteBuffer = serverResponse.getPayload().asReadOnlyByteBuffer();
                DataTable dataTable = DataTableFactory.getDataTable(byteBuffer);
                List<Object[]> rows = extractRows(dataTable);
                File inputFile = new File(localTempDir, "segment-" + segmentCount++ + ".csv");
                writeRowsToCSV(rows, dataTable.getDataSchema().getColumnNames(), inputFile);

                try {
                  SegmentGenerationTaskSpec taskSpec = generateTaskSpec(tableNameWithType, inputFile, localTempDir);
                  return generateAndPushSegment(taskSpec, resultBuilder, taskConfigs);
                } catch (Exception e) {
                  throw new RuntimeException("Failed to execute QueryToSegmentsTask", e);
                } finally {
                  // Cleanup output dir
                  FileUtils.deleteQuietly(localTempDir);
                }
              }
              catch (IOException e) {
                throw e;
              }
              continue;
            case CommonConstants.Query.Response.ResponseType.METADATA:
              // The last part of the response is Metadata
              serverResponseIterator = null;
              break;
            default:
              throw new Exception(String.format("Encountered Pinot exceptions, unknown response type - %s", responseType));
          }
      }
    }
    finally {
      if (byteBuffer != null) {
        byteBuffer.clear();
      }
    }
  }

  // copied from `reduceWithoutOrdering`
  private List<Object[]> extractRows(DataTable dataTable) {
    int numColumns = dataTable.getDataSchema().size();
    int numRows = dataTable.getNumberOfRows();
    List<Object[]> rows = new ArrayList<>();
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];;
    for (int coldId = 0; coldId < numColumns; coldId++) {
      nullBitmaps[coldId] = dataTable.getNullRowIds(coldId);
    }
    for (int rowId = 0; rowId < numRows; rowId++) {
      Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
      for (int colId = 0; colId < numColumns; colId++) {
        if (nullBitmaps[colId] != null && nullBitmaps[colId].contains(rowId)) {
          row[colId] = null;
        }
      }
      rows.add(row);
    }
    return rows;
  }

  // inspired from PinotSegmentToCsvConverter
  private void writeRowsToCSV(List<Object[]> rows, String[] columns, File outputFile) throws IOException {
    String delimiter = ",";
    String listDelimiter = ";";
    BufferedWriter recordWriter = new BufferedWriter(new FileWriter(outputFile));
    recordWriter.write(StringUtils.join(columns, delimiter));
    recordWriter.newLine();
    for (int index = 0; index < rows.size(); index++) {
      int numFields = columns.length;
      String[] values = new String[numFields];
      Object[] row = rows.get(index);
      for (int i = 0; i < numFields; i++) {
        Object value = row[i];
        if (value instanceof Object[]) {
          values[i] = StringUtils.join((Object[]) value, listDelimiter);
        } else if (value instanceof byte[]) {
          values[i] = BytesUtils.toHexString((byte[]) value);
        } else {
          values[i] = value.toString();
        }
      }
      recordWriter.write(StringUtils.join(values, delimiter));
      recordWriter.newLine();
    }
    recordWriter.close();
  }

  private QueryToSegmentsResult generateAndPushSegment(SegmentGenerationTaskSpec taskSpec,
      QueryToSegmentsResult.Builder resultBuilder, Map<String, String> taskConfigs)
      throws Exception {
    // Generate Pinot Segment
    _eventObserver.notifyProgress(_pinotTaskConfig, "Generating segment");
    SegmentGenerationTaskRunner taskRunner = new SegmentGenerationTaskRunner(taskSpec);
    String segmentName = taskRunner.run();

    // Tar segment directory to compress file
    _eventObserver.notifyProgress(_pinotTaskConfig, "Compressing segment: " + segmentName);
    File localSegmentTarFile = tarSegmentDir(taskSpec, segmentName);

    //move segment to output PinotFS
    _eventObserver.notifyProgress(_pinotTaskConfig, String.format("Moving segment: %s to output dir", segmentName));
    URI outputSegmentTarURI = moveSegmentToOutputPinotFS(taskConfigs, localSegmentTarFile);
    LOGGER.info("Moved generated segment from [{}] to location: [{}]", localSegmentTarFile, outputSegmentTarURI);

    resultBuilder.setSegmentName(segmentName);
    // Segment push task
    // TODO: Make this use SegmentUploader
    _eventObserver.notifyProgress(_pinotTaskConfig, "Pushing segment: " + segmentName);
    pushSegment(taskSpec.getTableConfig().getTableName(), taskConfigs, outputSegmentTarURI);
    resultBuilder.setSucceed(true);

    return resultBuilder.build();
  }

  private void pushSegment(String tableName, Map<String, String> taskConfigs, URI outputSegmentTarURI)
      throws Exception {
    String pushMode = taskConfigs.get(BatchConfigProperties.PUSH_MODE);
    LOGGER.info("Trying to push Pinot segment with push mode {} from {}", pushMode, outputSegmentTarURI);

    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setPushAttempts(DEFUALT_PUSH_ATTEMPTS);
    pushJobSpec.setPushParallelism(DEFAULT_PUSH_PARALLELISM);
    pushJobSpec.setPushRetryIntervalMillis(DEFAULT_PUSH_RETRY_INTERVAL_MILLIS);
    pushJobSpec.setSegmentUriPrefix(taskConfigs.get(BatchConfigProperties.PUSH_SEGMENT_URI_PREFIX));
    pushJobSpec.setSegmentUriSuffix(taskConfigs.get(BatchConfigProperties.PUSH_SEGMENT_URI_SUFFIX));

    SegmentGenerationJobSpec spec = generatePushJobSpec(tableName, taskConfigs, pushJobSpec);

    URI outputSegmentDirURI = null;
    if (taskConfigs.containsKey(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI)) {
      outputSegmentDirURI = URI.create(taskConfigs.get(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI));
    }
    try (PinotFS outputFileFS = MinionTaskUtils.getOutputPinotFS(taskConfigs, outputSegmentDirURI)) {
      switch (BatchConfigProperties.SegmentPushType.valueOf(pushMode.toUpperCase())) {
        case TAR:
          try (PinotFS pinotFS = MinionTaskUtils.getLocalPinotFs()) {
            SegmentPushUtils.pushSegments(spec, pinotFS, Arrays.asList(outputSegmentTarURI.toString()));
          } catch (RetriableOperationException | AttemptsExceededException e) {
            throw new RuntimeException(e);
          }
          break;
        case URI:
          try {
            List<String> segmentUris = new ArrayList<>();
            URI updatedURI = SegmentPushUtils.generateSegmentTarURI(outputSegmentDirURI, outputSegmentTarURI,
                pushJobSpec.getSegmentUriPrefix(), pushJobSpec.getSegmentUriSuffix());
            segmentUris.add(updatedURI.toString());
            SegmentPushUtils.sendSegmentUris(spec, segmentUris);
          } catch (RetriableOperationException | AttemptsExceededException e) {
            throw new RuntimeException(e);
          }
          break;
        case METADATA:
          try {
            Map<String, String> segmentUriToTarPathMap =
                SegmentPushUtils.getSegmentUriToTarPathMap(outputSegmentDirURI, pushJobSpec,
                    new String[]{outputSegmentTarURI.toString()});
            SegmentPushUtils.sendSegmentUriAndMetadata(spec, outputFileFS, segmentUriToTarPathMap);
          } catch (RetriableOperationException | AttemptsExceededException e) {
            throw new RuntimeException(e);
          }
          break;
        default:
          throw new UnsupportedOperationException("Unrecognized push mode - " + pushMode);
      }
    }
  }

  private SegmentGenerationJobSpec generatePushJobSpec(String tableName, Map<String, String> taskConfigs,
      PushJobSpec pushJobSpec) {

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(tableName);

    PinotClusterSpec pinotClusterSpec = new PinotClusterSpec();
    pinotClusterSpec.setControllerURI(taskConfigs.get(BatchConfigProperties.PUSH_CONTROLLER_URI));
    PinotClusterSpec[] pinotClusterSpecs = new PinotClusterSpec[]{pinotClusterSpec};

    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    spec.setPushJobSpec(pushJobSpec);
    spec.setTableSpec(tableSpec);
    spec.setPinotClusterSpecs(pinotClusterSpecs);
    spec.setAuthToken(taskConfigs.get(BatchConfigProperties.AUTH_TOKEN));

    return spec;
  }

  private URI moveSegmentToOutputPinotFS(Map<String, String> taskConfigs, File localSegmentTarFile)
      throws Exception {
    if (!taskConfigs.containsKey(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI)) {
      return localSegmentTarFile.toURI();
    }
    URI outputSegmentDirURI = URI.create(taskConfigs.get(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI));
    try (PinotFS outputFileFS = MinionTaskUtils.getOutputPinotFS(taskConfigs, outputSegmentDirURI)) {
      URI outputSegmentTarURI = URI.create(outputSegmentDirURI + localSegmentTarFile.getName());
      if (!Boolean.parseBoolean(taskConfigs.get(BatchConfigProperties.OVERWRITE_OUTPUT)) && outputFileFS.exists(
          outputSegmentDirURI)) {
        LOGGER.warn("Not overwrite existing output segment tar file: {}", outputFileFS.exists(outputSegmentDirURI));
      } else {
        outputFileFS.copyFromLocalFile(localSegmentTarFile, outputSegmentTarURI);
      }
      return outputSegmentTarURI;
    }
  }

  private File tarSegmentDir(SegmentGenerationTaskSpec taskSpec, String segmentName)
      throws IOException {
    File localOutputTempDir = new File(taskSpec.getOutputDirectoryPath());
    File localSegmentDir = new File(localOutputTempDir, segmentName);
    String segmentTarFileName = segmentName + Constants.TAR_GZ_FILE_EXT;
    File localSegmentTarFile = new File(localOutputTempDir, segmentTarFileName);
    LOGGER.info("Tarring segment from: {} to: {}", localSegmentDir, localSegmentTarFile);
    TarGzCompressionUtils.createTarGzFile(localSegmentDir, localSegmentTarFile);
    long uncompressedSegmentSize = FileUtils.sizeOf(localSegmentDir);
    long compressedSegmentSize = FileUtils.sizeOf(localSegmentTarFile);
    LOGGER.info("Size for segment: {}, uncompressed: {}, compressed: {}", segmentName,
        DataSizeUtils.fromBytes(uncompressedSegmentSize), DataSizeUtils.fromBytes(compressedSegmentSize));
    return localSegmentTarFile;
  }

  protected SegmentGenerationTaskSpec generateTaskSpec(String tableNameWithType, File inputFile, File localTempDir)
      throws Exception {
    SegmentGenerationTaskSpec taskSpec = new SegmentGenerationTaskSpec();
    URI inputFileURI = inputFile.toURI();

    File localOutputTempDir = new File(localTempDir, "output");
    FileUtils.forceMkdir(localOutputTempDir);
    taskSpec.setOutputDirectoryPath(localOutputTempDir.getAbsolutePath());
    taskSpec.setInputFilePath(inputFile.getAbsolutePath());

    RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
    recordReaderSpec.setDataFormat("csv");
    recordReaderSpec.setClassName("org.apache.pinot.plugin.inputformat.csv.CSVRecordReader");
    recordReaderSpec.setConfigClassName("org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig");
    taskSpec.setRecordReaderSpec(recordReaderSpec);

    Schema schema = getSchema(tableNameWithType);
    taskSpec.setSchema(schema);
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    taskSpec.setTableConfig(tableConfig);
    taskSpec.setFailOnEmptySegment(true);
    SegmentNameGeneratorSpec segmentNameGeneratorSpec = new SegmentNameGeneratorSpec();
    segmentNameGeneratorSpec.setType(BatchConfigProperties.SegmentNameGeneratorType.SIMPLE);
    segmentNameGeneratorSpec.setConfigs(new HashMap<String, String>());
    segmentNameGeneratorSpec.addConfig(SegmentGenerationTaskRunner.APPEND_UUID_TO_SEGMENT_NAME, "true");
    taskSpec.setSegmentNameGeneratorSpec(segmentNameGeneratorSpec);
    taskSpec.setCustomProperty(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY, inputFileURI.toString());

    return taskSpec;
  }
}
