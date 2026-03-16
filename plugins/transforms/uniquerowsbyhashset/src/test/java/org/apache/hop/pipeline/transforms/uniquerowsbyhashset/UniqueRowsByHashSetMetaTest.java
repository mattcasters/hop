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

package org.apache.hop.pipeline.transforms.uniquerowsbyhashset;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

class UniqueRowsByHashSetMetaTest {
  @Test
  void testLoadSave() throws Exception {
    Path path = Paths.get(Objects.requireNonNull(getClass().getResource("/transform.xml")).toURI());
    String xml = Files.readString(path);
    UniqueRowsByHashSetMeta meta = new UniqueRowsByHashSetMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        UniqueRowsByHashSetMeta.class,
        meta,
        new MemoryMetadataProvider());

    validate(meta);

    // Do a round trip:
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    UniqueRowsByHashSetMeta metaCopy = new UniqueRowsByHashSetMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        UniqueRowsByHashSetMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validate(metaCopy);
  }

  private static void validate(UniqueRowsByHashSetMeta meta) {
    assertNotNull(meta.getCompareFields());
    assertTrue(meta.isStoreValues());
    assertTrue(meta.isRejectDuplicateRow());
    assertEquals("Duplicate row detected", meta.getErrorDescription());
    assertEquals("compareField1", meta.getCompareFields().get(0).getName());
    assertEquals("compareField2", meta.getCompareFields().get(1).getName());
    assertEquals("compareField3", meta.getCompareFields().get(2).getName());
  }
}
