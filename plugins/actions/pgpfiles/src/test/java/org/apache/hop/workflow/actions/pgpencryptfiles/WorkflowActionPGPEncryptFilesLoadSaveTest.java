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

package org.apache.hop.workflow.actions.pgpencryptfiles;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class WorkflowActionPGPEncryptFilesLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionPGPEncryptFiles> {
  

  @Override
  protected Class<ActionPGPEncryptFiles> getActionClass() {
    return ActionPGPEncryptFiles.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] { "gpgLocation", "argFromPrevious", "includeSubFolders",
      "addResultFileNames", "destinationIsAFile", "createDestinationFolder", "addDate",
      "addTime", "specifyFormat", "dateTimeFormat", "nrErrorsLessThan", "successCondition",
      "addDateBeforeExtension", "doNotKeepFolderStructure", "ifFileExists", "destinationFolder",
      "ifMovedFileExists", "movedDateTimeFormat", "createMoveToFolder", "addMovedDate",
      "addMovedTime", "specifyMoveFormat", "addMovedDateBeforeExtension", "asciiMode",
      "actionType", "sourceFileFolder", "userId", "destinationFileFolder", "wildcard" } );
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, IFieldLoadSaveValidator<?>> validators = new HashMap<>();
    int count = new Random().nextInt( 50 ) + 1;

    validators.put( "actionType", new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( ActionPGPEncryptFiles.actionTypeCodes.length ), count ) );
    validators.put( "sourceFileFolder", new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), count ) );
    validators.put( "userId", new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), count ) );
    validators.put( "destinationFileFolder", new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), count ) );
    validators.put( "wildcard", new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), count ) );

    return validators;
  }
}
