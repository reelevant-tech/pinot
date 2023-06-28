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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.data.Segment;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * QueryToSegmentsTaskGenerator generates task configs for QueryToSegments minion tasks.
 *
 * This generator consumes following configs:
 *   query - Required, the SQL to execute
 *
 */
@TaskGenerator
public class QueryToSegmentsTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryToSegmentsTaskGenerator.class);
  private static final BatchConfigProperties.SegmentPushType DEFAULT_SEGMENT_PUSH_TYPE =
      BatchConfigProperties.SegmentPushType.TAR;

  @Override
  public String getTaskType() {
    return MinionConstants.QueryToSegmentsTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

    for (TableConfig tableConfig : tableConfigs) {
      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      Preconditions.checkNotNull(tableTaskConfig);
      Map<String, String> taskConfigs =
          tableTaskConfig.getConfigsForTaskType(MinionConstants.QueryToSegmentsTask.TASK_TYPE);
      Preconditions.checkNotNull(taskConfigs, "Task config shouldn't be null for Table: {}", tableConfig.getTableName());

      try {
        pinotTaskConfigs.addAll(generateTasks(tableConfig, taskConfigs));
      } catch (Exception e) {
        continue;
      }
    }
    return pinotTaskConfigs;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(TableConfig tableConfig, Map<String, String> taskConfigs)
      throws Exception {
    // Only generate tasks for OFFLINE tables
    String offlineTableName = tableConfig.getTableName();
    if (tableConfig.getTableType() != TableType.OFFLINE) {
      LOGGER.warn("Skip generating QueryToSegmentsTask for non-OFFLINE table: {}", offlineTableName);
      return ImmutableList.of();
    }

    // Override task configs from table with adhoc task configs.
    Map<String, String> batchConfigMap = new HashMap<>();
    TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
    if (tableTaskConfig != null) {
      batchConfigMap.putAll(
          tableTaskConfig.getConfigsForTaskType(MinionConstants.QueryToSegmentsTask.TASK_TYPE));
    }
    batchConfigMap.putAll(taskConfigs);

    try {
      updateRecordReaderConfigs(batchConfigMap);

      List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
      Map<String, String> singleFileGenerationTaskConfig = getTaskConfig(offlineTableName, batchConfigMap);
      pinotTaskConfigs.add(new PinotTaskConfig(MinionConstants.QueryToSegmentsTask.TASK_TYPE,
          singleFileGenerationTaskConfig));
      return pinotTaskConfigs;
    } catch (Exception e) {
      LOGGER.error("Unable to generate the QueryToSegments task. [ table configs: {}, task configs: {} ]",
          tableConfig, taskConfigs, e);
      throw e;
    }
  }

  private Map<String, String> getTaskConfig(String offlineTableName, Map<String, String> batchConfigMap)
      throws URISyntaxException {

    Set<Segment> runningSegments = TaskGeneratorUtils.getRunningSegments(MinionConstants.QueryToSegmentsTask.TASK_TYPE, _clusterInfoAccessor);

    Map<String, String> singleFileGenerationTaskConfig = new HashMap<>(batchConfigMap);
    singleFileGenerationTaskConfig
        .put(BatchConfigProperties.TABLE_NAME, TableNameBuilder.OFFLINE.tableNameWithType(offlineTableName));
    singleFileGenerationTaskConfig
        .put(BatchConfigProperties.SQL, batchConfigMap.get(BatchConfigProperties.SQL));
    singleFileGenerationTaskConfig.put(BatchConfigProperties.SEGMENTS, StringUtils.join(runningSegments, ";"));
    singleFileGenerationTaskConfig.put(BatchConfigProperties.PUSH_MODE, DEFAULT_SEGMENT_PUSH_TYPE.toString());
    singleFileGenerationTaskConfig.put(BatchConfigProperties.PUSH_CONTROLLER_URI, _clusterInfoAccessor.getVipUrl());

    return singleFileGenerationTaskConfig;
  }

  private void updateRecordReaderConfigs(Map<String, String> batchConfigMap) {
    String inputFormat = batchConfigMap.get(BatchConfigProperties.INPUT_FORMAT);
    String recordReaderClassName = PluginManager.get().getRecordReaderClassName(inputFormat);
    if (recordReaderClassName != null) {
      batchConfigMap.putIfAbsent(BatchConfigProperties.RECORD_READER_CLASS, recordReaderClassName);
    }
    String recordReaderConfigClassName = PluginManager.get().getRecordReaderConfigClassName(inputFormat);
    if (recordReaderConfigClassName != null) {
      batchConfigMap.putIfAbsent(BatchConfigProperties.RECORD_READER_CONFIG_CLASS, recordReaderConfigClassName);
    }
  }
}
