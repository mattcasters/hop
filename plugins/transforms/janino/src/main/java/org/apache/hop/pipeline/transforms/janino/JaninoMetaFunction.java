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
package org.apache.hop.pipeline.transforms.janino;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class JaninoMetaFunction implements Cloneable {
  @HopMetadataProperty(
      key = "field_name",
      injectionKey = "FIELD_NAME",
      injectionKeyDescription = "Janino.Injection.FIELD_NAME")
  private String fieldName;

  @HopMetadataProperty(
      key = "formula_string",
      injectionKey = "FIELD_FORMULA",
      injectionKeyDescription = "Janino.Injection.FIELD_FORMULA")
  private String formula;

  @HopMetadataProperty(
      key = "value_type",
      intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class,
      injectionKey = "VALUE_TYPE",
      injectionKeyDescription = "Janino.Injection.VALUE_TYPE")
  private int valueType;

  @HopMetadataProperty(
      key = "value_length",
      injectionKey = "VALUE_LENGTH",
      injectionKeyDescription = "Janino.Injection.VALUE_LENGTH")
  private int valueLength;

  @HopMetadataProperty(
      key = "value_precision",
      injectionKey = "VALUE_PRECISION",
      injectionKeyDescription = "Janino.Injection.VALUE_PRECISION")
  private int valuePrecision;

  @HopMetadataProperty(
      key = "replace_field",
      injectionKey = "REPLACE_FIELD",
      injectionKeyDescription = "Janino.Injection.REPLACE_FIELD")
  private String replaceField;

  public JaninoMetaFunction() {}

  public JaninoMetaFunction(JaninoMetaFunction f) {
    this();
    this.fieldName = f.fieldName;
    this.formula = f.formula;
    this.valueType = f.valueType;
    this.valueLength = f.valueLength;
    this.valuePrecision = f.valuePrecision;
    this.replaceField = f.replaceField;
  }

  public boolean equals(Object obj) {
    if (obj != null && (obj.getClass().equals(this.getClass()))) {
      JaninoMetaFunction mf = (JaninoMetaFunction) obj;
      return fieldName.equals(mf.getFieldName());
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, formula, valueType, valueLength, valuePrecision, replaceField);
  }

  @Override
  public Object clone() {
    return new JaninoMetaFunction(this);
  }
}
