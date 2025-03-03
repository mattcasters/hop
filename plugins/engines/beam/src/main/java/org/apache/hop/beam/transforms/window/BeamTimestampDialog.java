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

package org.apache.hop.beam.transforms.window;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class BeamTimestampDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamTimestampDialog.class;
  private final BeamTimestampMeta input;

  int middle;
  int margin;

  private Combo wFieldName;
  private Button wReading;

  public BeamTimestampDialog(
      Shell parent,
      IVariables variables,
      BeamTimestampMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "BeamTimestampDialog.DialogTitle"));

    middle = props.getMiddlePct();
    margin = PropsUi.getMargin();

    String[] fieldNames;
    try {
      fieldNames = pipelineMeta.getPrevTransformFields(variables, transformMeta).getFieldNames();
    } catch (HopException e) {
      log.logError("Error getting fields from previous transforms", e);
      fieldNames = new String[] {};
    }

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    Label wlFieldName = new Label(shell, SWT.RIGHT);
    wlFieldName.setText(BaseMessages.getString(PKG, "BeamTimestampDialog.FieldName"));
    PropsUi.setLook(wlFieldName);
    FormData fdlFieldName = new FormData();
    fdlFieldName.left = new FormAttachment(0, 0);
    fdlFieldName.top = new FormAttachment(lastControl, margin);
    fdlFieldName.right = new FormAttachment(middle, -margin);
    wlFieldName.setLayoutData(fdlFieldName);
    wFieldName = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFieldName);
    wFieldName.setItems(fieldNames);
    FormData fdFieldName = new FormData();
    fdFieldName.left = new FormAttachment(middle, 0);
    fdFieldName.top = new FormAttachment(wlFieldName, 0, SWT.CENTER);
    fdFieldName.right = new FormAttachment(100, 0);
    wFieldName.setLayoutData(fdFieldName);
    lastControl = wFieldName;

    Label wlReading = new Label(shell, SWT.RIGHT);
    wlReading.setText(BaseMessages.getString(PKG, "BeamTimestampDialog.Reading"));
    PropsUi.setLook(wlReading);
    FormData fdlReading = new FormData();
    fdlReading.left = new FormAttachment(0, 0);
    fdlReading.top = new FormAttachment(lastControl, margin);
    fdlReading.right = new FormAttachment(middle, -margin);
    wlReading.setLayoutData(fdlReading);
    wReading = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wReading);
    FormData fdReading = new FormData();
    fdReading.left = new FormAttachment(middle, 0);
    fdReading.top = new FormAttachment(wlReading, 0, SWT.CENTER);
    fdReading.right = new FormAttachment(100, 0);
    wReading.setLayoutData(fdReading);
    lastControl = wReading;

    // Buttons go at the very bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, margin, lastControl);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Populate the widgets. */
  public void getData() {
    wTransformName.setText(transformName);
    wFieldName.setText(Const.NVL(input.getFieldName(), ""));
    wReading.setSelection(input.isReadingTimestamp());

    wTransformName.selectAll();
    wTransformName.setFocus();
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

    getInfo(input);

    dispose();
  }

  private void getInfo(BeamTimestampMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setFieldName(wFieldName.getText());
    in.setReadingTimestamp(wReading.getSelection());

    input.setChanged();
  }
}
