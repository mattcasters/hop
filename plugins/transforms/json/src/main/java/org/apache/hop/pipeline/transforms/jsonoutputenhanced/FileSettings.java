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

package org.apache.hop.pipeline.transforms.jsonoutputenhanced;

import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class FileSettings {
  /** Flag: add the stepNr in the filename */
  @HopMetadataProperty(
      key = "split",
      injectionKey = "INC_TRANSFORMNR_IN_FILENAME",
      injectionKeyDescription = "JsonOutput.Injection.INC_TRANSFORMNR_IN_FILENAME")
  protected boolean transformNrInFilename;

  /** Flag: add the partition number in the filename */
  @HopMetadataProperty(
      key = "haspartno",
      injectionKey = "INC_PARTNR_IN_FILENAME",
      injectionKeyDescription = "JsonOutput.Injection.INC_PARTNR_IN_FILENAME")
  protected boolean partNrInFilename;

  /** Flag: add the date in the filename */
  @HopMetadataProperty(
      key = "add_date",
      injectionKey = "INC_DATE_IN_FILENAME",
      injectionKeyDescription = "JsonOutput.Injection.INC_DATE_IN_FILENAME")
  protected boolean dateInFileName;

  /** Flag: add the time in the filename */
  @HopMetadataProperty(
      key = "add_time",
      injectionKey = "INC_TIME_IN_FILENAME",
      injectionKeyDescription = "JsonOutput.Injection.INC_TIME_IN_FILENAME")
  protected boolean timeInFileName;

  /** The file extension in case of a generated filename */
  @HopMetadataProperty(
      key = "extension",
      injectionKey = "EXTENSION",
      injectionKeyDescription = "JsonOutput.Injection.EXTENSION")
  protected String extension;

  /** The base name of the output file */
  @HopMetadataProperty(
      key = "name",
      injectionKey = "FILENAME",
      injectionKeyDescription = "JsonOutput.Injection.FILENAME")
  protected String fileName;

  /** Whether to treat this as a command to be executed and piped into */
  @HopMetadataProperty(
      key = "fileAsCommand",
      injectionKey = "RUN_AS_COMMAND",
      injectionKeyDescription = "JsonOutput.Injection.RUN_AS_COMMAND")
  private boolean fileAsCommand;

  /** Flag : Do not open new file when transformation start */
  @HopMetadataProperty(
      key = "SpecifyFormat",
      injectionKey = "SPECIFY_DATE_FORMAT",
      injectionKeyDescription = "JsonOutput.Injection.SPECIFY_DATE_FORMAT")
  private boolean specifyingFormat;

  /** The date format appended to the file name */
  @HopMetadataProperty(
      key = "date_time_format",
      injectionKey = "DATE_FORMAT",
      injectionKeyDescription = "JsonOutput.Injection.DATE_FORMAT")
  private String dateTimeFormat;

  /** Choose if you want the output prittyfied */
  @HopMetadataProperty(
      key = "split_output_after",
      injectionKey = "SPLIT_OUTPUT_AFTER",
      injectionKeyDescription = "JsonOutput.Injection.SPLIT_OUTPUT_AFTER")
  protected int splitOutputAfter;

  /** The file compression: None, Zip or Gzip */
  @HopMetadataProperty(
      key = "compression",
      injectionKey = "COMPRESSION",
      injectionKeyDescription = "JsonOutput.Injection.COMPRESSION")
  private String fileCompression;

  /** Flag: create parent folder if needed */
  @HopMetadataProperty(
      key = "create_parent_folder",
      injectionKey = "CREATE_PARENT_FOLDER",
      injectionKeyDescription = "JsonOutput.Injection.")
  private boolean createParentFolder;

  /** Flag to indicate that we want to append to the end of an existing file (if it exists) */
  @HopMetadataProperty(
      key = "append",
      injectionKey = "APPEND",
      injectionKeyDescription = "JsonOutput.Injection.APPEND")
  private boolean fileAppended;

  @HopMetadataProperty(
      key = "doNotOpenNewFileInit",
      injectionKey = "",
      injectionKeyDescription = "JsonOutput.Injection.")
  private boolean doNotOpenNewFileInit;

  public FileSettings() {
    splitOutputAfter = 0;
  }

  public FileSettings(FileSettings f) {
    this();
    this.dateInFileName = f.dateInFileName;
    this.dateTimeFormat = f.dateTimeFormat;
    this.extension = f.extension;
    this.fileAsCommand = f.fileAsCommand;
    this.fileCompression = f.fileCompression;
    this.fileName = f.fileName;
    this.partNrInFilename = f.partNrInFilename;
    this.specifyingFormat = f.specifyingFormat;
    this.splitOutputAfter = f.splitOutputAfter;
    this.timeInFileName = f.timeInFileName;
    this.transformNrInFilename = f.transformNrInFilename;
    this.createParentFolder = f.createParentFolder;
    this.fileAppended = f.fileAppended;
    this.doNotOpenNewFileInit = f.doNotOpenNewFileInit;
  }

  public String buildFilename(
      final IVariables variables,
      final String copyNr,
      final String partitionNr,
      final String splitNr,
      final boolean ziparchive) {
    return buildFilename(variables, copyNr, partitionNr, splitNr, ziparchive, true);
  }

  public String buildFilename(
      final IVariables variables,
      final String stepNr,
      final String partNr,
      final String splitNr,
      final boolean ziparchive,
      final boolean showSamples) {

    String realFileName = variables.resolve(fileName);
    String realExtension = variables.resolve(extension);
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String retval = realFileName;

    if (isFileAsCommand()) {
      return retval;
    }

    Date now = new Date();

    if (isSpecifyingFormat() && !Utils.isEmpty(getDateTimeFormat())) {
      daf.applyPattern(getDateTimeFormat());
      String dt = daf.format(now);
      retval += dt;
    } else {
      if (isDateInFileName()) {
        if (showSamples) {
          daf.applyPattern("yyyMMdd");
          String d = daf.format(now);
          retval += "_" + d;
        } else {
          retval += "_<yyyMMdd>";
        }
      }
      if (isTimeInFileName()) {
        if (showSamples) {
          daf.applyPattern("HHmmss");
          String t = daf.format(now);
          retval += "_" + t;
        } else {
          retval += "_<HHmmss>";
        }
      }
    }
    if (getSplitOutputAfter() > 0) {
      retval += "_" + splitNr;
    }

    if (isTransformNrInFilename()) {
      retval += "_" + stepNr;
    }
    if (isPartNrInFilename()) {
      retval += "_" + partNr;
    }

    if ("Zip".equalsIgnoreCase(getFileCompression())) {
      if (ziparchive) {
        retval += ".zip";
      } else {
        if (!Utils.isEmpty(realExtension)) {
          retval += "." + realExtension;
        }
      }
    } else {
      if (!Utils.isEmpty(realExtension)) {
        retval += "." + realExtension;
      }
      if ("GZip".equalsIgnoreCase(getFileCompression())) {
        retval += ".gz";
      }
    }
    return retval;
  }

  public String[] getFiles(IVariables variables, String partitionId, final boolean showSamples) {
    if (showSamples) {
      int copies = 1;
      int splits = 1;
      int parts = 1;

      if (isTransformNrInFilename()) {
        copies = 3;
      }

      if (isPartNrInFilename()) {
        parts = 3;
      }

      int nr = copies * parts * splits;
      if (nr > 1) {
        nr++;
      }

      String[] retval = new String[nr];

      int i = 0;
      for (int copy = 0; copy < copies; copy++) {
        for (int part = 0; part < parts; part++) {
          for (int split = 0; split < splits; split++) {
            retval[i] =
                buildFilename(variables, copy + "", partitionId, split + "", false, showSamples);
            i++;
          }
        }
      }
      if (i < nr) {
        retval[i] = "...";
      }

      return retval;
    } else {
      return new String[] {
        buildFilename(variables, "<transform>", "<partition>", "<split>", false, showSamples)
      };
    }
  }
}
