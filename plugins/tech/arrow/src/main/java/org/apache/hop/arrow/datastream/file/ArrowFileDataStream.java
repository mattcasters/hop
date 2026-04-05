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

package org.apache.hop.arrow.datastream.file;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.stream.metadata.DataStreamMeta;
import org.apache.hop.stream.plugin.DataStreamPlugin;
import org.apache.hop.stream.plugin.IDataStream;

@GuiPlugin
@DataStreamPlugin(
    id = "ArrowFile",
    name = "Apache Arrow File Stream",
    description = "Stream rows of data to an Apache Arrow stream file")
@Getter
@Setter
public class ArrowFileDataStream implements IDataStream {
  @GuiWidgetElement(
      order = "20000-arrow-file-data-stream-filename",
      parentId = DataStreamMeta.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::ArrowFileDataStream.Filename.Label",
      toolTip = "i18n::ArrowFileDataStream.Filename.Tooltip")
  @HopMetadataProperty
  private String filename;

  @GuiWidgetElement(
      order = "20100-arrow-file-data-stream-buffer-size",
      parentId = DataStreamMeta.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::ArrowFileDataStream.BufferSize.Label",
      toolTip = "i18n::ArrowFileDataStream.BufferSize.Tooltip")
  @HopMetadataProperty
  private String bufferSize;

  private String pluginId;
  private String pluginName;

  private boolean writing;

  private IVariables variables;
  private IHopMetadataProvider metadataProvider;
  private BufferAllocator rootAllocator;
  private VectorSchemaRoot vectorSchemaRoot;
  private IRowMeta rowMeta;
  private List<Object[]> rowBuffer;
  private String realFilename;
  private int realBufferSize;
  private FileOutputStream fileOutputStream;
  private ArrowStreamWriter arrowFileWriter;

  private FileInputStream fileInputStream;
  private ArrowFileReader arrowFileReader;
  private Schema readSchema;
  private VectorSchemaRoot readBatch;
  private int readRowIndex;

  public ArrowFileDataStream() {
    DataStreamPlugin annotation = getClass().getAnnotation(DataStreamPlugin.class);
    this.pluginId = annotation.id();
    this.pluginName = annotation.name();
    rowBuffer = new ArrayList<>();
    bufferSize = "1000";
    filename = "${java.io.tmpdir}/file-stream.arrow";
  }

  @SuppressWarnings("CopyConstructorMissesField")
  public ArrowFileDataStream(ArrowFileDataStream a) {
    this();
    this.filename = a.filename;
    this.bufferSize = a.bufferSize;
  }

  @Override
  public ArrowFileDataStream clone() {
    return new ArrowFileDataStream(this);
  }

  @Override
  public void initialize(
      IVariables variables, IHopMetadataProvider metadataProvider, boolean writing) {
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.writing = writing;

    rowBuffer = new ArrayList<>();
    rootAllocator = new RootAllocator();

    realFilename = variables.resolve(filename);
    realBufferSize = Const.toInt(variables.resolve(bufferSize), 1000);
  }

  @Override
  public void setOutputDone() throws HopException {
    if (!rowBuffer.isEmpty()) {
      emptyBuffer();
    }
    try {
      arrowFileWriter.end();
    } catch (Exception e) {
      throw new HopException("Error ending arrow file stream", e);
    }
  }

  @Override
  public void close() {
    vectorSchemaRoot.close();
    rootAllocator.close();
    if (arrowFileWriter != null) {
      arrowFileWriter.close();
    }
    if (arrowFileReader != null) {
      try {
        arrowFileReader.close();
      } catch (IOException e) {
        // Ignore
      }
    }
    if (fileOutputStream != null) {
      try {
        fileOutputStream.close();
      } catch (Exception e) {
        // Ignore
      }
    }
    if (fileInputStream != null) {
      try {
        fileInputStream.close();
      } catch (IOException e) {
        // Ignore
      }
    }
  }

  @Override
  public void setRowMeta(IRowMeta rowMeta) throws HopException {
    if (!writing) {
      return;
    }
    this.rowMeta = rowMeta;

    List<Field> fields = new ArrayList<>();
    for (IValueMeta valueMeta : rowMeta.getValueMetaList()) {
      String name = valueMeta.getName();
      FieldType fieldType =
          switch (valueMeta.getType()) {
            case IValueMeta.TYPE_STRING -> FieldType.nullable(new ArrowType.Utf8());
            case IValueMeta.TYPE_BOOLEAN -> FieldType.nullable(new ArrowType.Bool());
            case IValueMeta.TYPE_INTEGER -> FieldType.nullable(new ArrowType.Int(64, true));
            case IValueMeta.TYPE_NUMBER ->
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            case IValueMeta.TYPE_BIGNUMBER ->
                FieldType.nullable(
                    new ArrowType.Decimal(valueMeta.getLength(), valueMeta.getPrecision(), 64));
            case IValueMeta.TYPE_DATE ->
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
            default ->
                throw new HopException(
                    "Data streaming to a file with Apache Arrow isn't yet supported for Hop data type "
                        + valueMeta.getTypeDesc());
          };
      Field field = new Field(name, fieldType, null);
      fields.add(field);
    }
    Schema writeSchema = new Schema(fields);
    vectorSchemaRoot = VectorSchemaRoot.create(writeSchema, rootAllocator);
    try {
      fileOutputStream = new FileOutputStream(variables.resolve(filename));
    } catch (Exception e) {
      throw new HopException("Error writing to file output stream", e);
    }
    arrowFileWriter = new ArrowStreamWriter(vectorSchemaRoot, null, fileOutputStream.getChannel());
    try {
      arrowFileWriter.start();
    } catch (Exception e) {
      throw new HopException("Error startig to write to file output stream", e);
    }
  }

  /**
   * Gets rowMeta
   *
   * @return value of rowMeta
   */
  public IRowMeta getRowMeta() throws HopException {
    if (writing) {
      return rowMeta;
    }
    try {
      fileInputStream = new FileInputStream(realFilename);
      arrowFileReader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator);
      VectorSchemaRoot rootSchema = arrowFileReader.getVectorSchemaRoot();
      readSchema = rootSchema.getSchema();
      this.rowMeta = new RowMeta();
      for (Field field : readSchema.getFields()) {
        int hopType =
            switch (field.getType().getTypeID()) {
              case Utf8 -> IValueMeta.TYPE_STRING;
              case Int -> IValueMeta.TYPE_INTEGER;
              case Bool -> IValueMeta.TYPE_BOOLEAN;
              case Timestamp -> IValueMeta.TYPE_DATE;
              case Decimal -> IValueMeta.TYPE_BIGNUMBER;
              case FloatingPoint -> IValueMeta.TYPE_NUMBER;
              default ->
                  throw new HopException(
                      "Unsupported data type ID found in stream for field "
                          + field.getName()
                          + " : "
                          + field.getType().getTypeID().name());
            };
        IValueMeta valueMeta = ValueMetaFactory.createValueMeta(field.getName(), hopType);
        this.rowMeta.addValueMeta(valueMeta);

        // Create a read batch.  This batch will be filled regularly.
        readBatch = arrowFileReader.getVectorSchemaRoot();
        readRowIndex = 0;
      }
    } catch (Exception e) {
      throw new HopException(
          "Error reading row metadata from Apache Arrow streaming file " + realFilename, e);
    }
    return this.rowMeta;
  }

  @Override
  public void writeRow(Object[] rowData) throws HopException {
    rowBuffer.add(rowData);
    if (rowBuffer.size() >= realBufferSize) {
      emptyBuffer();
    }
  }

  private void emptyBuffer() throws HopException {
    try {
      for (int rowIndex = 0; rowIndex < rowBuffer.size(); rowIndex++) {
        Object[] rowData = rowBuffer.get(rowIndex);
        for (int fieldIndex = 0; fieldIndex < rowMeta.size(); fieldIndex++) {
          IValueMeta valueMeta = rowMeta.getValueMeta(fieldIndex);
          Object valueData = valueMeta.getNativeDataType(rowData[fieldIndex]);
          if (valueMeta.isNull(valueData)) {
            vectorSchemaRoot.getVector(fieldIndex).setNull(fieldIndex);
            continue;
          }
          switch (valueMeta.getType()) {
            case IValueMeta.TYPE_STRING, IValueMeta.TYPE_JSON ->
                addString(rowIndex, fieldIndex, valueMeta, valueData);
            case IValueMeta.TYPE_INTEGER -> addInteger(rowIndex, fieldIndex, valueMeta, valueData);
            case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER ->
                addBigNumber(rowIndex, fieldIndex, valueMeta, valueData);
            case IValueMeta.TYPE_BOOLEAN -> addBoolean(rowIndex, fieldIndex, valueMeta, valueData);
            case IValueMeta.TYPE_DATE -> addDate(rowIndex, fieldIndex, valueMeta, valueData);
            default ->
                throw new HopException(
                    "Data streaming to an Apache Arrow stream file isn't yet supported for Hop data type "
                        + valueMeta.getTypeDesc());
          }
        }
      }
      arrowFileWriter.writeBatch();
    } catch (Exception e) {
      throw new HopException("Error writing row to Apache Arrow stream file " + filename, e);
    } finally {
      // We're done. Fill the buffer up again.
      rowBuffer.clear();
    }
  }

  private void addString(int rowIndex, int fieldIndex, IValueMeta valueMeta, Object valueData)
      throws HopException {
    VarCharVector vector = (VarCharVector) vectorSchemaRoot.getVector(fieldIndex);
    if (rowIndex == 0) {
      vector.allocateNew(rowBuffer.size());
    }
    vector.set(rowIndex, valueMeta.getString(valueData).getBytes());
  }

  private void addInteger(int rowIndex, int fieldIndex, IValueMeta valueMeta, Object valueData)
      throws HopException {
    BigIntVector vector = (BigIntVector) vectorSchemaRoot.getVector(fieldIndex);
    if (rowIndex == 0) {
      vector.allocateNew(rowBuffer.size());
    }
    vector.set(rowIndex, valueMeta.getInteger(valueData));
  }

  private void addBigNumber(int rowIndex, int fieldIndex, IValueMeta valueMeta, Object valueData)
      throws HopException {
    DecimalVector vector = (DecimalVector) vectorSchemaRoot.getVector(fieldIndex);
    if (rowIndex == 0) {
      vector.allocateNew(rowBuffer.size());
    }
    vector.set(rowIndex, valueMeta.getBigNumber(valueData));
  }

  private void addBoolean(int rowIndex, int fieldIndex, IValueMeta valueMeta, Object valueData)
      throws HopException {
    BitVector vector = (BitVector) vectorSchemaRoot.getVector(fieldIndex);
    if (rowIndex == 0) {
      vector.allocateNew(rowBuffer.size());
    }
    vector.set(rowIndex, Boolean.TRUE.equals(valueMeta.getBoolean(valueData)) ? 1 : 0);
  }

  private void addDate(int rowIndex, int fieldIndex, IValueMeta valueMeta, Object valueData)
      throws HopException {
    BigIntVector vector = (BigIntVector) vectorSchemaRoot.getVector(fieldIndex);
    if (rowIndex == 0) {
      vector.allocateNew(rowBuffer.size());
    }
    vector.set(rowIndex, valueMeta.getDate(valueData).getTime());
  }

  @Override
  public Object[] readRow() throws HopException {
    if (writing) {
      throw new HopException("When writing data you can't read rows from the same data stream.");
    }

    try {
      // See if there are more rows to populate.
      //
      int batchSize = readBatch.getRowCount();
      if (readRowIndex < batchSize) {
        return buildReadRow(readRowIndex++);
      }
      // Read another batch
      readRowIndex = 0;
      if (arrowFileReader.loadNextBatch()) {
        return buildReadRow(readRowIndex++);
      } else {
        // No more data to be expected
        return null;
      }
    } catch (Exception e) {
      throw new HopException(
          "Error while reading a batch of rows from an Apache Arrow file stream", e);
    }
  }

  private Object[] buildReadRow(int rowIndex) throws HopException {
    Object[] rowData = RowDataUtil.allocateRowData(rowBuffer.size());
    for (int fieldIndex = 0; fieldIndex < rowMeta.size(); fieldIndex++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(fieldIndex);
      Object valueData;
      FieldVector fieldVector = readBatch.getFieldVectors().get(fieldIndex);
      if (fieldVector.isNull(rowIndex)) {
        valueData = null;
      } else {
        valueData =
            switch (valueMeta.getType()) {
              case IValueMeta.TYPE_STRING -> getString(rowIndex, fieldIndex);
              case IValueMeta.TYPE_BOOLEAN -> getBoolean(rowIndex, fieldIndex);
              case IValueMeta.TYPE_INTEGER -> getInteger(rowIndex, fieldIndex);
              case IValueMeta.TYPE_NUMBER -> getNumber(rowIndex, fieldIndex);
              case IValueMeta.TYPE_BIGNUMBER -> getBigNumber(rowIndex, fieldIndex);
              case IValueMeta.TYPE_DATE -> getDate(rowIndex, fieldIndex);
              default ->
                  throw new HopException(
                      "Reading value "
                          + fieldIndex
                          + " of type "
                          + valueMeta.getTypeDesc()
                          + " is not yet supported");
            };
      }
      rowData[fieldIndex] = valueData;
    }
    return rowData;
  }

  private String getString(int rowIndex, int fieldIndex) {
    VarCharVector vector = (VarCharVector) readBatch.getFieldVectors().get(fieldIndex);
    return new String(vector.get(rowIndex));
  }

  private Boolean getBoolean(int rowIndex, int fieldIndex) {
    BitVector vector = (BitVector) readBatch.getFieldVectors().get(fieldIndex);
    return vector.get(rowIndex) == 1;
  }

  private Long getInteger(int rowIndex, int fieldIndex) {
    BigIntVector vector = (BigIntVector) readBatch.getFieldVectors().get(fieldIndex);
    return vector.get(rowIndex);
  }

  private Double getNumber(int rowIndex, int fieldIndex) {
    FloatingPointVector vector = (FloatingPointVector) readBatch.getFieldVectors().get(fieldIndex);
    return vector.getValueAsDouble(rowIndex);
  }

  private Date getDate(int rowIndex, int fieldIndex) {
    TimeStampVector vector = (TimeStampVector) readBatch.getFieldVectors().get(fieldIndex);
    return new Date(vector.get(rowIndex));
  }

  private BigDecimal getBigNumber(int rowIndex, int fieldIndex) {
    DecimalVector vector = (DecimalVector) readBatch.getFieldVectors().get(fieldIndex);
    return vector.getObject(rowIndex);
  }
}
