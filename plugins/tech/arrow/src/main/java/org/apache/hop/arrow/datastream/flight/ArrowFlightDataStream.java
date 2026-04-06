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

package org.apache.hop.arrow.datastream.flight;

import java.util.ArrayList;
import lombok.Getter;
import lombok.Setter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.arrow.datastream.shared.ArrowBaseDataStream;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datastream.metadata.DataStreamMeta;
import org.apache.hop.datastream.plugin.DataStreamPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.staticschema.metadata.SchemaDefinition;

@DataStreamPlugin(
    id = "ArrowFlightStream",
    name = "Apache Arrow Flight Stream",
    description =
        "Reference this stream name when sending to, or reading from, the Hop Arrow Flight server")
@Getter
@Setter
@GuiPlugin
public class ArrowFlightDataStream extends ArrowBaseDataStream {
  @GuiWidgetElement(
      order = "20000-arrow-flight-data-stream-buffer-size",
      parentId = DataStreamMeta.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::ArrowFlightDataStream.BufferSize.Label",
      toolTip = "i18n::ArrowFlightDataStream.BufferSize.Tooltip")
  @HopMetadataProperty
  protected String bufferSize;

  @GuiWidgetElement(
      order = "20100-arrow-flight-data-stream-batch-size",
      parentId = DataStreamMeta.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::ArrowFlightDataStream.BatchSize.Label",
      toolTip = "i18n::ArrowFlightDataStream.BatchSize.Tooltip")
  @HopMetadataProperty
  protected String batchSize;

  @GuiWidgetElement(
      order = "20200-arrow-flight-data-stream-schema-definition",
      parentId = DataStreamMeta.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.METADATA,
      metadata = SchemaDefinition.class,
      label = "i18n::ArrowFlightDataStream.SchemaDefinition.Label",
      toolTip = "i18n::ArrowFlightDataStream.SchemaDefinition.Tooltip")
  @HopMetadataProperty(key = "schemaDefinition")
  protected String schemaDefinitionName;

  private int realBufferSize;
  private int realBatchSize;
  private SchemaDefinition schemaDefinition;

  private IRowSet rowSet;

  public ArrowFlightDataStream() {
    DataStreamPlugin annotation = getClass().getAnnotation(DataStreamPlugin.class);
    this.pluginId = annotation.id();
    this.pluginName = annotation.name();
    rowBuffer = new ArrayList<>();
    bufferSize = "500";
    batchSize = "10000";
  }

  @SuppressWarnings("CopyConstructorMissesField")
  public ArrowFlightDataStream(ArrowFlightDataStream s) {
    this();
    this.bufferSize = s.bufferSize;
    this.batchSize = s.batchSize;
    this.schemaDefinitionName = s.schemaDefinitionName;
  }

  @Override
  public ArrowFlightDataStream clone() {
    return new ArrowFlightDataStream(this);
  }

  @Override
  public void initialize(
      IVariables variables, IHopMetadataProvider metadataProvider, boolean writing)
      throws HopException {
    super.initialize(variables, metadataProvider, writing);
    realBufferSize = Const.toInt(variables.resolve(bufferSize), 10000);
    realBatchSize = Const.toInt(variables.resolve(batchSize), 500);
    String realSchemaDefinition = variables.resolve(schemaDefinitionName);
    schemaDefinition =
        metadataProvider.getSerializer(SchemaDefinition.class).load(realSchemaDefinition);

    // Allocate the row set buffer
    rowSet = new BlockingRowSet(realBufferSize);
  }

  @Override
  public void setRowMeta(IRowMeta rowMeta) {
    rowSet.setRowMeta(rowMeta);
  }

  @Override
  public void close() {
    // Nothing to close
  }

  @Override
  public void writeRow(Object[] rowData) {
    // Put the row in the row set, possibly blocking.
    rowSet.putRow(rowMeta, rowData);
  }

  @Override
  public void setOutputDone() {
    // Signal downstream that we're done with this row set.
    rowSet.setDone();
  }

  @Override
  public Object[] readRow() {
    // Get a row from the row set buffer, possibly blocking
    return rowSet.getRow();
  }

  public IRowMeta buildExpectedRowMeta() throws HopException {
    return schemaDefinition.getRowMeta();
  }

  public Schema buildExpectedSchema() throws HopException {
    return ArrowBaseDataStream.buildSchema(schemaDefinition.getRowMeta());
  }
}
