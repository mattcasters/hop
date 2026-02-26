/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.dbimpact.input;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.LabelComboVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Shell;

public class DbImpactInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = DbImpactInputMeta.class;

  private LabelComboVar wFileNameField;

  private final DbImpactInputMeta input;

  public DbImpactInputDialog(
      Shell parent,
      IVariables variables,
      DbImpactInputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "DbImpactInputMeta.Name"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    wFileNameField =
        new LabelComboVar(
            variables,
            shell,
            BaseMessages.getString(PKG, "DbImpactInputMeta.FileNameField.Label"),
            BaseMessages.getString(PKG, "DbImpactInputDialog.FileNameField.Tooltip"));
    PropsUi.setLook(wFileNameField);
    FormData fdFileNameField = new FormData();
    fdFileNameField.left = new FormAttachment(0, 0);
    fdFileNameField.top = new FormAttachment(wSpacer, margin);
    fdFileNameField.bottom = new FormAttachment(wOk, -2 * margin);
    fdFileNameField.right = new FormAttachment(100, 0);
    wFileNameField.setLayoutData(fdFileNameField);

    getData();

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wFileNameField.setText(Const.NVL(input.getFileNameField(), ""));

    try {
      String[] fieldNames =
          pipelineMeta.getPrevTransformFields(getVariables(), transformName).getFieldNames();
      wFileNameField.setItems(fieldNames);
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting field names from previous transforms", e);
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo(input);

    dispose();
  }

  private void getInfo(DbImpactInputMeta meta) {
    meta.setFileNameField(wFileNameField.getText());
  }
}
