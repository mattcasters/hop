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

import java.io.File;
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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
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
import org.apache.hop.datastream.metadata.DataStreamMeta;
import org.apache.hop.datastream.plugin.DataStreamPlugin;
import org.apache.hop.datastream.plugin.IDataStream;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;

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
  private ArrowStreamWriter arrowStreamWriter;

  private FileInputStream fileInputStream;
  private ArrowStreamReader arrowStreamReader;
  private VectorSchemaRoot readRootSchema;
  private Schema readSchema;
  private int readRowIndex;
  private List<FieldVector> readFieldVectors;
  private int batchReads;

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
      arrowStreamWriter.end();
    } catch (Exception e) {
      throw new HopException("Error ending arrow file stream", e);
    }
  }

  @Override
  public void close() {
    if (arrowStreamReader != null) {
      try {
        arrowStreamReader.close();
      } catch (IOException e) {
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
    if (vectorSchemaRoot != null) {
      vectorSchemaRoot.close();
    }
    if (arrowStreamWriter != null) {
      arrowStreamWriter.close();
    }
    if (fileOutputStream != null) {
      try {
        fileOutputStream.close();
      } catch (IOException e) {
        // Ignore
      }
    }

    if (rootAllocator != null) {
      rootAllocator.close();
    }
  }

  @Override
  public void setRowMeta(IRowMeta rowMeta) throws HopException {
    if (!writing) {
      return;
    }
    this.rowMeta = rowMeta;

    initializeStreamWriting();
  }

  private void initializeStreamWriting() throws HopException {
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

    // Allocate room in the field vectors
    //
    allocateFieldVectorsSpace();

    try {
      fileOutputStream = new FileOutputStream(variables.resolve(filename));
    } catch (Exception e) {
      throw new HopException("Error writing to file output stream", e);
    }
    arrowStreamWriter =
        new ArrowStreamWriter(vectorSchemaRoot, null, fileOutputStream.getChannel());
    try {
      arrowStreamWriter.start();
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
      initializeStreamReading();
    } catch (Exception e) {
      throw new HopException(
          "Error reading row metadata from Apache Arrow streaming file " + realFilename, e);
    }
    return this.rowMeta;
  }

  private void initializeStreamReading() throws HopException, IOException {
    File file = new File(realFilename);
    if (!file.exists()) {
      throw new HopException("The Arrow stream file to read from doesn't exist: " + realFilename);
    }

    fileInputStream = new FileInputStream(realFilename);
    arrowStreamReader = new ArrowStreamReader(fileInputStream, rootAllocator);
    readRootSchema = arrowStreamReader.getVectorSchemaRoot();
    readSchema = readRootSchema.getSchema();

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
      readRowIndex = 0;
    }
  }

  private void allocateFieldVectorsSpace() throws HopException {
    for (int fieldIndex = 0; fieldIndex < rowMeta.size(); fieldIndex++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(fieldIndex);
      FieldVector fieldVector = vectorSchemaRoot.getVector(fieldIndex);

      // Set all values for the field vector
      //
      switch (valueMeta.getType()) {
        case IValueMeta.TYPE_STRING -> ((VarCharVector) fieldVector).allocateNew(realBufferSize);
        case IValueMeta.TYPE_INTEGER -> ((BigIntVector) fieldVector).allocateNew(realBufferSize);
        case IValueMeta.TYPE_NUMBER -> ((Float8Vector) fieldVector).allocateNew(realBufferSize);
        case IValueMeta.TYPE_BIGNUMBER -> ((DecimalVector) fieldVector).allocateNew(realBufferSize);
        case IValueMeta.TYPE_BOOLEAN -> ((BitVector) fieldVector).allocateNew(realBufferSize);
        case IValueMeta.TYPE_DATE -> ((TimeStampVector) fieldVector).allocateNew(realBufferSize);
        default ->
            throw new HopException(
                "Allocating space for field vector isn't supported yet for data type "
                    + valueMeta.getTypeDesc());
      }
    }
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
      vectorSchemaRoot.setRowCount(rowBuffer.size());

      // Set the data in the field vectors for the rows in the buffer
      //
      for (int rowIndex = 0; rowIndex < rowBuffer.size(); rowIndex++) {
        Object[] rowData = rowBuffer.get(rowIndex);

        for (int fieldIndex = 0; fieldIndex < rowMeta.size(); fieldIndex++) {
          IValueMeta valueMeta = rowMeta.getValueMeta(fieldIndex);
          FieldVector fieldVector = vectorSchemaRoot.getVector(fieldIndex);

          // Set all values for the field vector
          //
          Object valueData = valueMeta.getNativeDataType(rowData[fieldIndex]);
          switch (valueMeta.getType()) {
            case IValueMeta.TYPE_STRING -> addString(rowIndex, fieldVector, valueMeta, valueData);
            case IValueMeta.TYPE_INTEGER -> addInteger(rowIndex, fieldVector, valueMeta, valueData);
            case IValueMeta.TYPE_NUMBER -> addNumber(rowIndex, fieldVector, valueMeta, valueData);
            case IValueMeta.TYPE_BIGNUMBER ->
                addBigNumber(rowIndex, fieldVector, valueMeta, valueData);
            case IValueMeta.TYPE_BOOLEAN -> addBoolean(rowIndex, fieldVector, valueMeta, valueData);
            case IValueMeta.TYPE_DATE -> addDate(rowIndex, fieldVector, valueMeta, valueData);
            default ->
                throw new HopException(
                    "Data streaming to an Apache Arrow stream file isn't yet supported for Hop data type "
                        + valueMeta.getTypeDesc());
          }
        }
      }
      // With values set on all field vectors, we can now write the batch.
      //
      arrowStreamWriter.writeBatch();
    } catch (Exception e) {
      throw new HopException("Error writing row to Apache Arrow stream file " + filename, e);
    } finally {
      // We're done. Fill the buffer up again.
      rowBuffer.clear();
    }
  }

  private void addString(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, Object valueData)
      throws HopException {
    VarCharVector vector = (VarCharVector) fieldVector;
    // Check the size of the vector
    //
    if (valueMeta.isNull(valueData)) {
      vector.setNull(rowIndex);
    } else {
      try {
        vector.set(rowIndex, valueMeta.getString(valueData).getBytes());
      } catch (Exception e) {
        throw new HopException(e.getMessage());
      }
    }
  }

  private void addInteger(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, Object valueData)
      throws HopException {
    BigIntVector vector = (BigIntVector) fieldVector;
    if (valueMeta.isNull(valueData)) {
      vector.setNull(rowIndex);
    } else {
      vector.set(rowIndex, valueMeta.getInteger(valueData));
    }
  }

  private void addNumber(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, Object valueData)
      throws HopException {
    Float8Vector vector = (Float8Vector) fieldVector;
    if (valueMeta.isNull(valueData)) {
      vector.setNull(rowIndex);
    } else {
      vector.set(rowIndex, valueMeta.getNumber(valueData));
    }
  }

  private void addBigNumber(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, Object valueData)
      throws HopException {
    DecimalVector vector = (DecimalVector) fieldVector;
    if (valueMeta.isNull(valueData)) {
      vector.setNull(rowIndex);
    } else {
      vector.set(rowIndex, valueMeta.getBigNumber(valueData));
    }
  }

  private void addBoolean(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, Object valueData)
      throws HopException {
    BitVector vector = (BitVector) fieldVector;
    if (valueMeta.isNull(valueData)) {
      vector.setNull(rowIndex);
    } else {
      vector.set(rowIndex, Boolean.TRUE.equals(valueMeta.getBoolean(valueData)) ? 1 : 0);
    }
  }

  private void addDate(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, Object valueData)
      throws HopException {
    TimeStampMilliTZVector vector = (TimeStampMilliTZVector) fieldVector;
    if (valueMeta.isNull(valueData)) {
      vector.setNull(rowIndex);
    } else {
      vector.set(rowIndex, valueMeta.getDate(valueData).getTime());
    }
  }

  @Override
  public Object[] readRow() throws HopException {
    if (writing) {
      throw new HopException("When writing data you can't read rows from the same data stream.");
    }

    try {
      // See if there are more rows to populate.
      //
      int batchSize = readRootSchema.getRowCount();
      if (readRowIndex < batchSize) {
        return buildReadRow(readRowIndex++);
      }
      // Read another batch
      readRowIndex = 0;
      if (readNextBatch()) {
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

  private boolean readNextBatch() throws IOException {
    boolean readNext = arrowStreamReader.loadNextBatch();
    batchReads++;
    readRootSchema = arrowStreamReader.getVectorSchemaRoot();
    while (readRootSchema.getRowCount() == 0 && readNext) {
      readNext = arrowStreamReader.loadNextBatch();
      batchReads++;
      readRootSchema = arrowStreamReader.getVectorSchemaRoot();
    }
    readFieldVectors = readRootSchema.getFieldVectors();

    return readNext;
  }

  private Object[] buildReadRow(int rowIndex) throws HopException {
    Object[] rowData = RowDataUtil.allocateRowData(rowBuffer.size());

    for (int fieldIndex = 0; fieldIndex < rowMeta.size(); fieldIndex++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(fieldIndex);
      Object valueData;
      FieldVector fieldVector = readFieldVectors.get(fieldIndex);
      if (fieldVector.isNull(rowIndex)) {
        valueData = null;
      } else {
        valueData =
            switch (valueMeta.getType()) {
              case IValueMeta.TYPE_STRING -> getString(rowIndex, fieldVector);
              case IValueMeta.TYPE_BOOLEAN -> getBoolean(rowIndex, fieldVector);
              case IValueMeta.TYPE_INTEGER -> getInteger(rowIndex, fieldVector);
              case IValueMeta.TYPE_NUMBER -> getNumber(rowIndex, fieldVector);
              case IValueMeta.TYPE_BIGNUMBER -> getBigNumber(rowIndex, fieldVector);
              case IValueMeta.TYPE_DATE -> getDate(rowIndex, fieldVector);
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

  private String getString(int rowIndex, FieldVector fieldVector) {
    VarCharVector vector = (VarCharVector) fieldVector;
    return new String(vector.get(rowIndex));
  }

  private Boolean getBoolean(int rowIndex, FieldVector fieldVector) {
    BitVector vector = (BitVector) fieldVector;
    return vector.get(rowIndex) == 1;
  }

  private Long getInteger(int rowIndex, FieldVector fieldVector) {
    BigIntVector vector = (BigIntVector) fieldVector;
    return vector.get(rowIndex);
  }

  private Double getNumber(int rowIndex, FieldVector fieldVector) {
    FloatingPointVector vector = (FloatingPointVector) fieldVector;
    return vector.getValueAsDouble(rowIndex);
  }

  private Date getDate(int rowIndex, FieldVector fieldVector) {
    TimeStampVector vector = (TimeStampVector) fieldVector;
    return new Date(vector.get(rowIndex));
  }

  private BigDecimal getBigNumber(int rowIndex, FieldVector fieldVector) {
    DecimalVector vector = (DecimalVector) fieldVector;
    return vector.getObject(rowIndex);
  }
}
