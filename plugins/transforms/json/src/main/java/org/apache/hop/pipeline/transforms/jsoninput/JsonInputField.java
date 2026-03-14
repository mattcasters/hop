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

package org.apache.hop.pipeline.transforms.jsoninput;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.ITextFileInputField;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transforms.file.BaseFileField;

/** Describes a JsonPath field. */
@Getter
@Setter
public class JsonInputField extends BaseFileField implements ITextFileInputField {
  public static final String CONST_SPACES = "        ";

  @HopMetadataProperty(
      key = "path",
      injectionKey = "FIELD_PATH",
      injectionKeyDescription = "JsonInput.Injection.FIELD_PATH")
  private String path;

  public JsonInputField(String fieldName) {
    super();
    setName(fieldName);
  }

  public JsonInputField() {
    this("");
  }

  public JsonInputField(JsonInputField f) {
    super(f);
    this.path = f.path;
  }

  @Override
  public JsonInputField clone() {
    return new JsonInputField(this);
  }
}
