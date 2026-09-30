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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.CONTROLLER_HOST;
import static org.apache.pinot.controller.ControllerConf.CONTROLLER_PORT;
import static org.apache.pinot.spi.utils.CommonConstants.Controller.CONFIG_OF_INSTANCE_ID;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.CONTROLLER_INSTANCE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


@Test(groups = "stateless")
public class ControllerStarterStatelessTest extends ControllerTest {
  private final Map<String, Object> _configOverride = new HashMap<>();

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.putAll(_configOverride);
  }

  @Test
  public void testHostnamePortOverride()
      throws Exception {
    _configOverride.clear();
    _configOverride.put(CONFIG_OF_INSTANCE_ID, "Controller_myInstance");
    _configOverride.put(CONTROLLER_HOST, "myHost");
    _configOverride.put(CONTROLLER_PORT, 1234);

    startZk();
    startController();

    String instanceId = _controllerStarter.getInstanceId();
    assertEquals(instanceId, "Controller_myInstance");
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, instanceId);
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), "1234");
    assertEquals(instanceConfig.getTags(), Collections.singleton(CONTROLLER_INSTANCE));

    stopController();
    stopZk();
  }

  @Test
  public void testInvalidInstanceId()
      throws Exception {
    _configOverride.clear();
    _configOverride.put(CONFIG_OF_INSTANCE_ID, "myInstance");
    _configOverride.put(CONTROLLER_HOST, "myHost");
    _configOverride.put(CONTROLLER_PORT, 1234);

    startZk();
    try {
      startController();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    } finally {
      // The starter was created before init() failed, so later tests would see a started controller
      _controllerStarter = null;
      stopZk();
    }
  }

  @Test
  public void testDefaultInstanceId()
      throws Exception {
    _configOverride.clear();
    _configOverride.put(CONTROLLER_HOST, "myHost");
    _configOverride.put(CONTROLLER_PORT, 1234);

    startZk();
    startController();

    String instanceId = _controllerStarter.getInstanceId();
    assertEquals(instanceId, "Controller_myHost_1234");
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, instanceId);
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), "1234");
    assertEquals(instanceConfig.getTags(), Collections.singleton(CONTROLLER_INSTANCE));

    stopController();
    stopZk();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testParticipantDoesNotWatchResourceConfigs()
      throws Exception {
    _configOverride.clear();

    startZk();
    startController();
    try {
      // Read on start without a Helix callback
      assertTrue(_controllerStarter.getLeadControllerManager().isLeadControllerResourceEnabled());

      HelixManager participantManager = _controllerStarter.getHelixResourceManager().getHelixZkManager();
      assertEquals(participantManager.getInstanceType(), InstanceType.PARTICIPANT);
      // Helix 1.3.2 has no public accessor for the registered callback handlers
      Field handlersField = ZKHelixManager.class.getDeclaredField("_handlers");
      handlersField.setAccessible(true);
      List<String> watchedPaths = new ArrayList<>();
      synchronized (participantManager) {
        for (CallbackHandler handler : (List<CallbackHandler>) handlersField.get(participantManager)) {
          watchedPaths.add(handler.getPath());
        }
      }
      assertFalse(watchedPaths.isEmpty());
      String resourceConfigsPath = new PropertyKey.Builder(getHelixClusterName()).resourceConfigs().getPath();
      assertFalse(watchedPaths.contains(resourceConfigsPath),
          "Participant must not watch " + resourceConfigsPath + ", got: " + watchedPaths);
    } finally {
      stopController();
      stopZk();
    }
  }
}
