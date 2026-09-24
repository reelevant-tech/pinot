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
package org.apache.pinot.controller;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class LeadControllerManagerTest {
  private static final String HELIX_CONTROLLER_INSTANCE_ID = "localhost_18998";
  private static final long FAST_FETCH_INTERVAL_MS = 50L;
  private static final long TIMEOUT_MS = 10_000L;

  private HelixManager _helixManager;
  private ControllerMetrics _controllerMetrics;
  private LiveInstance _liveInstance;
  // Read by the mocks through answers, so that the fetching thread never races with a re-stubbing
  private volatile boolean _resourceEnabled;
  private volatile boolean _failResourceConfigRead;

  @BeforeMethod
  public void setup() {
    _resourceEnabled = false;
    _failResourceConfigRead = false;
    _controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    when(_helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);

    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    when(helixDataAccessor.keyBuilder()).thenReturn(keyBuilder);

    PropertyKey controllerLeader = mock(PropertyKey.class);
    when(keyBuilder.controllerLeader()).thenReturn(controllerLeader);
    _liveInstance = mock(LiveInstance.class);
    when(helixDataAccessor.getProperty(controllerLeader)).thenReturn(_liveInstance);

    ConfigAccessor configAccessor = mock(ConfigAccessor.class);
    when(_helixManager.getConfigAccessor()).thenReturn(configAccessor);
    ResourceConfig resourceConfig = mock(ResourceConfig.class);
    when(configAccessor.getResourceConfig(any(), anyString())).thenAnswer(invocation -> {
      if (_failResourceConfigRead) {
        throw new RuntimeException("Simulated ZK failure");
      }
      return resourceConfig;
    });
    when(resourceConfig.getSimpleConfig(anyString())).thenAnswer(invocation -> Boolean.toString(_resourceEnabled));
  }

  @Test
  public void testLeadControllerManager() {
    LeadControllerManager leadControllerManager =
        new LeadControllerManager(HELIX_CONTROLLER_INSTANCE_ID, _helixManager, _controllerMetrics);
    String tableName = "leadControllerTestTable";
    int expectedPartitionIndex = LeadControllerUtils.getPartitionIdForTable(tableName);
    String partitionName = LeadControllerUtils.generatePartitionName(expectedPartitionIndex);

    becomeHelixLeader(false);
    leadControllerManager.onHelixControllerChange();

    // When there's no resource config change nor helix controller change, leadControllerManager should return false.
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));

    enableResourceConfig(true);
    leadControllerManager.refreshLeadControllerResourceEnabled();

    // Even resource config is enabled, leadControllerManager should return false because no index is cached yet.
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));
    Assert.assertTrue(LeadControllerUtils.isLeadControllerResourceEnabled(_helixManager));

    // After the target partition index is cached, leadControllerManager should return true.
    leadControllerManager.addPartitionLeader(partitionName);
    Assert.assertTrue(leadControllerManager.isLeaderForTable(tableName));

    // When the target partition index is removed, leadControllerManager should return false.
    leadControllerManager.removePartitionLeader(partitionName);
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));

    // When resource config is set to false, the cache should be disabled, even if the target partition index is in
    // the cache.
    // The leader depends on whether the current controller is helix leader.
    enableResourceConfig(false);
    leadControllerManager.refreshLeadControllerResourceEnabled();

    Assert.assertFalse(LeadControllerUtils.isLeadControllerResourceEnabled(_helixManager));
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));
    leadControllerManager.addPartitionLeader(partitionName);
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));

    // When the current controller becomes helix leader and resource is disabled, leadControllerManager should return
    // true.
    becomeHelixLeader(true);
    leadControllerManager.onHelixControllerChange();
    Assert.assertTrue(leadControllerManager.isLeaderForTable(tableName));
  }

  @Test
  public void testResourceConfigReadOnStart() {
    enableResourceConfig(true);
    // An interval longer than the test, so only start() and the thread's first iteration read the config
    LeadControllerManager leadControllerManager =
        new LeadControllerManager(HELIX_CONTROLLER_INSTANCE_ID, _helixManager, _controllerMetrics, 3_600_000L);
    Assert.assertFalse(leadControllerManager.isLeadControllerResourceEnabled());

    leadControllerManager.start();
    try {
      // start() reads it synchronously, without any Helix callback
      Assert.assertTrue(leadControllerManager.isLeadControllerResourceEnabled());
      Assert.assertEquals(getResourceEnabledGauge(), Long.valueOf(1L));
    } finally {
      leadControllerManager.stop();
    }
  }

  @Test
  public void testResourceConfigRefreshedByFetchingThread() {
    String tableName = "leadControllerTestTable";
    String partitionName =
        LeadControllerUtils.generatePartitionName(LeadControllerUtils.getPartitionIdForTable(tableName));
    LeadControllerManager leadControllerManager =
        new LeadControllerManager(HELIX_CONTROLLER_INSTANCE_ID, _helixManager, _controllerMetrics,
            FAST_FETCH_INTERVAL_MS);
    leadControllerManager.start();
    try {
      Assert.assertFalse(leadControllerManager.isLeadControllerResourceEnabled());
      Assert.assertEquals(getResourceEnabledGauge(), Long.valueOf(0L));
      leadControllerManager.addPartitionLeader(partitionName);
      // Not the Helix leader and the resource is disabled
      Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));

      enableResourceConfig(true);
      TestUtils.waitForCondition(
          aVoid -> leadControllerManager.isLeaderForTable(tableName) && getResourceEnabledGauge() == 1L, TIMEOUT_MS,
          "Fetching thread did not pick up the enabled lead controller resource");

      enableResourceConfig(false);
      TestUtils.waitForCondition(
          aVoid -> !leadControllerManager.isLeaderForTable(tableName) && getResourceEnabledGauge() == 0L, TIMEOUT_MS,
          "Fetching thread did not pick up the disabled lead controller resource");
    } finally {
      leadControllerManager.stop();
    }
  }

  @Test
  public void testResourceConfigKeptOnReadFailure() {
    enableResourceConfig(true);
    LeadControllerManager leadControllerManager =
        new LeadControllerManager(HELIX_CONTROLLER_INSTANCE_ID, _helixManager, _controllerMetrics);
    leadControllerManager.refreshLeadControllerResourceEnabled();
    Assert.assertTrue(leadControllerManager.isLeadControllerResourceEnabled());

    // A failed read must not regress the flag, even if ZK now says disabled
    enableResourceConfig(false);
    _failResourceConfigRead = true;
    leadControllerManager.refreshLeadControllerResourceEnabled();
    Assert.assertTrue(leadControllerManager.isLeadControllerResourceEnabled());

    _failResourceConfigRead = false;
    leadControllerManager.refreshLeadControllerResourceEnabled();
    Assert.assertFalse(leadControllerManager.isLeadControllerResourceEnabled());
  }

  private void becomeHelixLeader(boolean becomeHelixLeader) {
    if (becomeHelixLeader) {
      when(_liveInstance.getInstanceName()).thenReturn(HELIX_CONTROLLER_INSTANCE_ID);
    }
  }

  private void enableResourceConfig(boolean enable) {
    _resourceEnabled = enable;
  }

  private Long getResourceEnabledGauge() {
    return _controllerMetrics.getGaugeValue(ControllerGauge.PINOT_LEAD_CONTROLLER_RESOURCE_ENABLED.getGaugeName());
  }
}
