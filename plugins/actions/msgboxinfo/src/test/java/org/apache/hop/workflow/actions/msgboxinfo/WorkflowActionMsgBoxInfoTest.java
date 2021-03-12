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
 */
package org.apache.hop.workflow.actions.msgboxinfo;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WorkflowActionMsgBoxInfoTest extends WorkflowActionLoadSaveTestSupport<ActionMsgBoxInfo> {
  

  @Override
  protected Class<ActionMsgBoxInfo> getActionClass() {
    return ActionMsgBoxInfo.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
      "bodymessage",
      "titremessage" );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
      "bodymessage", "getBodyMessage",
      "titremessage", "getTitleMessage" );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
      "bodymessage", "setBodyMessage",
      "titremessage", "setTitleMessage" );
  }

}
