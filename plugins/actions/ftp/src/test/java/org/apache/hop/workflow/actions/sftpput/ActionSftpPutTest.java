/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.workflow.actions.sftpput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActionSftpPutTest {
  @BeforeEach
  void beforeEach() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    ActionSftpPut action =
        ActionSerializationTestUtil.testSerialization("/action-sftp-put.xml", ActionSftpPut.class);

    assertEquals("server", action.getServerName());
    assertEquals("22", action.getServerPort());
    assertEquals("username", action.getUserName());
    assertEquals("password", action.getPassword());
    assertEquals("remote-folder", action.getRemoteDirectory());
    assertEquals("local-dir", action.getLocalDirectory());
    assertEquals("wildcard", action.getWildcard());
    assertTrue(action.isCopyingPrevious());
    assertTrue(action.isCopyingPreviousFiles());
    assertTrue(action.isAddFilenameResut());
    assertTrue(action.isUseKeyFilename());
    assertEquals("keyfile", action.getKeyFilename());
    assertEquals("keypass", action.getKeyFilePassword());
    assertEquals("zlib", action.getCompression());
    assertEquals("HTTP", action.getProxyType());
    assertEquals("proxy-host", action.getProxyHost());
    assertEquals("80", action.getProxyPort());
    assertEquals("proxy-user", action.getProxyUsername());
    assertEquals("proxy-pass", action.getProxyPassword());
    assertTrue(action.isCreateRemoteFolder());
    assertEquals(ActionSftpPut.AfterFtpAction.MOVE, action.getAfterSftpAction());
    assertEquals("move-to-folder", action.getDestinationFolder());
    assertTrue(action.isCreateDestinationFolder());
    assertTrue(action.isPreserveTargetFileTimestamp());
    assertTrue(action.isSuccessWhenNoFile());
  }
}
