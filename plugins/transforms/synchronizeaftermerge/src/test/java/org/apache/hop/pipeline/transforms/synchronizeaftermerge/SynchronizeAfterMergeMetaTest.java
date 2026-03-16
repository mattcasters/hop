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
package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

class SynchronizeAfterMergeMetaTest {
  @Test
  void testLoadSave() throws Exception {
    Path path = Paths.get(Objects.requireNonNull(getClass().getResource("/transform.xml")).toURI());
    String xml = Files.readString(path);
    SynchronizeAfterMergeMeta meta = new SynchronizeAfterMergeMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        SynchronizeAfterMergeMeta.class,
        meta,
        new MemoryMetadataProvider());

    validate(meta);

    // Do a round trip:
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    SynchronizeAfterMergeMeta metaCopy = new SynchronizeAfterMergeMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        SynchronizeAfterMergeMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validate(metaCopy);
  }

  private static void validate(SynchronizeAfterMergeMeta m) {
    assertEquals("connectionName", m.getConnection());
    assertEquals("123", m.getCommitSize());
    assertEquals("sourceField", m.getTableNameField());
    assertTrue(m.isTableNameInField());
    assertTrue(m.isUsingBatchUpdates());
    assertTrue(m.isPerformingLookup());
    assertEquals("operationFieldName", m.getOperationOrderField());
    assertEquals("insert", m.getOrderInsert());
    assertEquals("update", m.getOrderUpdate());
    assertEquals("delete", m.getOrderDelete());

    SynchronizeAfterMergeMeta.Lookup lookup = m.getLookup();
    assertNotNull(lookup);
    assertEquals("targetSchema", lookup.getSchemaName());
    assertEquals("targetTable", lookup.getTableName());
    assertNotNull(lookup.getKeyConditions());
    assertEquals(1, lookup.getKeyConditions().size());
    SynchronizeAfterMergeMeta.KeyCondition k1 = lookup.getKeyConditions().get(0);
    assertEquals("keyColumn1", k1.getColumnName());
    assertEquals("keyField1", k1.getFieldName());
    assertEquals("=", k1.getCondition());
    assertNotNull(lookup.getValueUpdates());
    assertEquals(4, lookup.getValueUpdates().size());
    SynchronizeAfterMergeMeta.ValueUpdate v1 = lookup.getValueUpdates().get(0);
    assertEquals("column1", v1.getColumnName());
    assertEquals("field1", v1.getFieldName());
    assertTrue(v1.isUpdate());
    SynchronizeAfterMergeMeta.ValueUpdate v2 = lookup.getValueUpdates().get(1);
    assertEquals("column2", v2.getColumnName());
    assertEquals("field2", v2.getFieldName());
    assertFalse(v2.isUpdate());
    SynchronizeAfterMergeMeta.ValueUpdate v3 = lookup.getValueUpdates().get(2);
    assertEquals("column3", v3.getColumnName());
    assertEquals("field3", v3.getFieldName());
    assertTrue(v3.isUpdate());
    SynchronizeAfterMergeMeta.ValueUpdate v4 = lookup.getValueUpdates().get(3);
    assertEquals("column4", v4.getColumnName());
    assertEquals("field4", v4.getFieldName());
    assertFalse(v4.isUpdate());
  }
}
