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

package org.apache.hop.projects.gui;

import java.io.File;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.git.GitRepoProvider;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.GitCloneHelper;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class CloneFromVersionControlDialog extends Dialog {
  private static final Class<?> PKG = CloneFromVersionControlDialog.class;

  private String returnValue;
  private Shell shell;
  private final PropsUi props;
  private final IVariables variables;

  private Text wUrl;
  private TextVar wDirectory;
  private Text wProjectName;
  private Text wToken;
  private Button wShallowClone;
  private Text wShallowDepth;

  public CloneFromVersionControlDialog(Shell parent, IVariables variables) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    this.variables = new Variables();
    this.variables.initializeFrom(variables);
    props = PropsUi.getInstance();
  }

  public String open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setImage(
        GuiResource.getInstance()
            .getImage(
                "project.svg",
                PKG.getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE));

    PropsUi.setLook(shell);

    int margin = PropsUi.getMargin() + 2;
    int middle = props.getMiddlePct();

    shell.setLayout(new FormLayout());
    shell.setText(BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Shell.Name"));

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, event -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, event -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin * 3, null);

    Composite comp = new Composite(shell, SWT.NONE);
    comp.setLayout(new FormLayout());
    PropsUi.setLook(comp);

    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(0, 0);
    fdComp.right = new FormAttachment(100, 0);
    fdComp.top = new FormAttachment(0, 0);
    fdComp.bottom = new FormAttachment(wOk, -margin);
    comp.setLayoutData(fdComp);

    Control lastControl = null;

    // URL
    Label wlUrl = new Label(comp, SWT.RIGHT);
    wlUrl.setText(BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.Url"));
    FormData fdlUrl = new FormData();
    fdlUrl.left = new FormAttachment(0, 0);
    fdlUrl.right = new FormAttachment(middle, 0);
    fdlUrl.top = new FormAttachment(0, margin * 2);
    wlUrl.setLayoutData(fdlUrl);
    Button wBrowseRepo = new Button(comp, SWT.PUSH);
    wBrowseRepo.setText(
        BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Button.BrowseRepo"));
    FormData fdbBrowseRepo = new FormData();
    fdbBrowseRepo.right = new FormAttachment(99, 0);
    fdbBrowseRepo.top = new FormAttachment(wlUrl, 0, SWT.CENTER);
    wBrowseRepo.setLayoutData(fdbBrowseRepo);
    wBrowseRepo.addListener(SWT.Selection, e -> browseRepository());
    wUrl = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    FormData fdUrl = new FormData();
    fdUrl.left = new FormAttachment(middle, margin);
    fdUrl.right = new FormAttachment(wBrowseRepo, -margin);
    fdUrl.top = new FormAttachment(wlUrl, 0, SWT.CENTER);
    wUrl.setLayoutData(fdUrl);
    lastControl = wUrl;

    // Token (optional) — placed directly below the URL / Browse row
    Label wlToken = new Label(comp, SWT.RIGHT);
    wlToken.setText(BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.Token"));
    FormData fdlToken = new FormData();
    fdlToken.left = new FormAttachment(0, 0);
    fdlToken.right = new FormAttachment(middle, 0);
    fdlToken.top = new FormAttachment(lastControl, margin);
    wlToken.setLayoutData(fdlToken);
    wToken = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT | SWT.PASSWORD);
    PropsUi.setLook(wToken);
    FormData fdToken = new FormData();
    fdToken.left = new FormAttachment(middle, margin);
    fdToken.right = new FormAttachment(99, 0);
    fdToken.top = new FormAttachment(wlToken, 0, SWT.CENTER);
    wToken.setLayoutData(fdToken);
    wToken.setToolTipText(
        BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.Token.Tooltip"));
    lastControl = wToken;

    // Directory
    Label wlDir = new Label(comp, SWT.RIGHT);
    wlDir.setText(BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.Directory"));
    FormData fdlDir = new FormData();
    fdlDir.left = new FormAttachment(0, 0);
    fdlDir.right = new FormAttachment(middle, 0);
    fdlDir.top = new FormAttachment(lastControl, margin);
    wlDir.setLayoutData(fdlDir);
    Button wbDir = new Button(comp, SWT.PUSH);
    wbDir.setText(BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Button.Browse"));
    FormData fdbDir = new FormData();
    fdbDir.right = new FormAttachment(99, 0);
    fdbDir.top = new FormAttachment(wlDir, 0, SWT.CENTER);
    wbDir.setLayoutData(fdbDir);
    wbDir.addListener(SWT.Selection, e -> browseDirectory());
    wDirectory = new TextVar(variables, comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    FormData fdDir = new FormData();
    fdDir.left = new FormAttachment(middle, margin);
    fdDir.right = new FormAttachment(wbDir, -margin);
    fdDir.top = new FormAttachment(wlDir, 0, SWT.CENTER);
    wDirectory.setLayoutData(fdDir);
    lastControl = wDirectory;

    // Project name
    Label wlName = new Label(comp, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.ProjectName"));
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    fdlName.top = new FormAttachment(lastControl, margin);
    wlName.setLayoutData(fdlName);
    wProjectName = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, margin);
    fdName.right = new FormAttachment(99, 0);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    wProjectName.setLayoutData(fdName);
    wUrl.addListener(SWT.Modify, e -> updateProjectNameFromUrl());
    lastControl = wProjectName;

    // Shallow clone
    Label wlShallow = new Label(comp, SWT.RIGHT);
    wlShallow.setText(
        BaseMessages.getString(PKG, "CloneFromVersionControlDialog.Label.ShallowClone"));
    FormData fdlShallow = new FormData();
    fdlShallow.left = new FormAttachment(0, 0);
    fdlShallow.right = new FormAttachment(middle, 0);
    fdlShallow.top = new FormAttachment(lastControl, margin * 2);
    wlShallow.setLayoutData(fdlShallow);
    wShallowClone = new Button(comp, SWT.CHECK);
    FormData fdShallow = new FormData();
    fdShallow.left = new FormAttachment(middle, margin);
    fdShallow.top = new FormAttachment(wlShallow, 0, SWT.CENTER);
    wShallowClone.setLayoutData(fdShallow);
    wShallowClone.addListener(SWT.Selection, e -> updateShallowDepthState());
    wShallowDepth = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wShallowDepth);
    wShallowDepth.setText("1");
    FormData fdDepth = new FormData();
    fdDepth.left = new FormAttachment(wShallowClone, margin);
    fdDepth.top = new FormAttachment(wlShallow, 0, SWT.CENTER);
    fdDepth.width = 60;
    wShallowDepth.setLayoutData(fdDepth);
    Label wlCommits = new Label(comp, SWT.LEFT);
    wlCommits.setText(
        " "
            + BaseMessages.getString(
                PKG, "CloneFromVersionControlDialog.Label.ShallowClone.Commits"));
    FormData fdlCommits = new FormData();
    fdlCommits.left = new FormAttachment(wShallowDepth, margin);
    fdlCommits.top = new FormAttachment(wlShallow, 0, SWT.CENTER);
    wlCommits.setLayoutData(fdlCommits);

    getData();
    updateShallowDepthState();

    shell.setDefaultButton(wOk);
    wUrl.setFocus();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return returnValue;
  }

  private void browseRepository() {
    GitRepoProvider detectedProvider = GitRepoProvider.detectFromUrl(wUrl.getText().trim());
    SelectRepositoryDialog dialog =
        new SelectRepositoryDialog(shell, detectedProvider, wToken.getText().trim());
    String cloneUrl = dialog.open();
    if (cloneUrl != null) {
      wUrl.setText(cloneUrl);
      String repoName = dialog.getSelectedRepoName();
      if (repoName != null && StringUtils.isEmpty(wProjectName.getText())) {
        wProjectName.setText(repoName);
      }
    }
  }

  private void updateShallowDepthState() {
    wShallowDepth.setEnabled(wShallowClone.getSelection());
  }

  private void updateProjectNameFromUrl() {
    if (StringUtils.isEmpty(wProjectName.getText())) {
      String url = wUrl.getText();
      if (StringUtils.isNotEmpty(url)) {
        String baseName = FilenameUtils.getBaseName(url.replaceFirst("\\.git$", ""));
        if (StringUtils.isNotEmpty(baseName)) {
          wProjectName.setText(baseName);
        }
      }
    }
  }

  private void browseDirectory() {
    BaseDialog.presentDirectoryDialog(shell, wDirectory, variables);
  }

  private void getData() {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    String standardFolder = variables.resolve(config.getStandardProjectsFolder());
    wDirectory.setText(Const.NVL(standardFolder, ""));
  }

  private void ok() {
    try {
      String url = wUrl.getText().trim();
      if (StringUtils.isEmpty(url)) {
        throw new HopException("Please specify the repository URL");
      }
      String directory = variables.resolve(wDirectory.getText());
      if (StringUtils.isEmpty(directory)) {
        throw new HopException("Please specify the directory");
      }
      String projectName = wProjectName.getText().trim();
      if (StringUtils.isEmpty(projectName)) {
        projectName = FilenameUtils.getBaseName(url.replaceFirst("\\.git$", ""));
      }
      if (StringUtils.isEmpty(projectName)) {
        throw new HopException("Please specify a project name");
      }

      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      if (config.findProjectConfig(projectName) != null) {
        throw new HopException("Project '" + projectName + "' already exists");
      }

      String clonePath = directory + File.separator + projectName;
      FileObject cloneDir = HopVfs.getFileObject(clonePath);
      if (cloneDir.exists()) {
        throw new HopException("Directory '" + clonePath + "' already exists");
      }

      int depth = 0;
      if (wShallowClone.getSelection()) {
        try {
          depth = Integer.parseInt(wShallowDepth.getText().trim());
          if (depth < 1) {
            depth = 1;
          }
        } catch (NumberFormatException e) {
          depth = 1;
        }
      }

      if (!GitCloneHelper.cloneRepo(clonePath, url, wToken.getText(), depth)) {
        return;
      }

      String defaultConfigFile = variables.resolve(config.getDefaultProjectConfigFile());
      ProjectConfig projectConfig = new ProjectConfig(projectName, clonePath, defaultConfigFile);
      Project project = new Project();
      project.setParentProjectName(config.getStandardParentProject());

      String configFilename = projectConfig.getActualProjectConfigFilename(variables);
      FileObject configFile = HopVfs.getFileObject(configFilename);
      if (!configFile.exists()) {
        project.setConfigFilename(configFilename);
        if (!configFile.getParent().exists()) {
          configFile.getParent().createFolder();
        }
        project.saveToFile();
      } else {
        project.setConfigFilename(configFilename);
        project.readFromFile();
      }

      config.addProjectConfig(projectConfig);
      HopConfig.getInstance().saveToFile();

      HopGui hopGui = HopGui.getInstance();
      ProjectsGuiPlugin.updateProjectToolItem(projectName);
      ProjectsGuiPlugin.enableHopGuiProject(projectName, project, null);

      returnValue = projectName;
      dispose();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              ProjectsGuiPlugin.PKG, "ProjectGuiPlugin.AddProject.Error.Dialog.Header"),
          BaseMessages.getString(
              ProjectsGuiPlugin.PKG, "ProjectGuiPlugin.AddProject.Error.Dialog.Message"),
          e);
    }
  }

  private void cancel() {
    returnValue = null;
    dispose();
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
