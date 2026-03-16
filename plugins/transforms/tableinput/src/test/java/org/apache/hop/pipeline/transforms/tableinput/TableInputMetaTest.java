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

package org.apache.hop.pipeline.transforms.tableinput;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TableInputMetaTest {
  @Test
  void testLoadSave() throws Exception {
    Path path = Paths.get(Objects.requireNonNull(getClass().getResource("/transform.xml")).toURI());
    String xml = Files.readString(path);
    TableInputMeta meta = new TableInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        TableInputMeta.class,
        meta,
        new MemoryMetadataProvider());

    validate(meta);

    // Do a round trip:
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    TableInputMeta metaCopy = new TableInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        TableInputMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validate(metaCopy);
  }

  private static void validate(TableInputMeta meta) {
    Assertions.assertEquals("h2", meta.getConnection());
    Assertions.assertEquals("100", meta.getRowLimit());
    Assertions.assertEquals("parameters", meta.getLookup());
    Assertions.assertTrue(meta.isExecuteEachInputRow());
    Assertions.assertTrue(meta.isVariableReplacementActive());
    Assertions.assertEquals("SELECT ID, NAME FROM PUBLIC.DDLTEST WHERE NAME = ?", meta.getSql());

    // Do we have an IO stream?
    Assertions.assertFalse(meta.getTransformIOMeta().getInfoStreams().isEmpty());
    IStream stream = meta.getTransformIOMeta().getInfoStreams().get(0);
    Assertions.assertNotNull(stream);
    Assertions.assertEquals("parameters", stream.getSubject());
  }
}
