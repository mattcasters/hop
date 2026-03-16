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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JaninoMetaTest {
  @BeforeEach
  void beforeEach() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    String[] classNames = {
      ValueMetaString.class.getName(), ValueMetaInteger.class.getName(),
      ValueMetaDate.class.getName(), ValueMetaNumber.class.getName()
    };
    for (String className : classNames) {
      registry.registerPluginClass(className, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
  }

  @Test
  void testLoadSave() throws Exception {
    Path path = Paths.get(Objects.requireNonNull(getClass().getResource("/janino.xml")).toURI());
    String xml = Files.readString(path);
    JaninoMeta meta = new JaninoMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        JaninoMeta.class,
        meta,
        new MemoryMetadataProvider());

    validate(meta);

    // Do a round trip:
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    JaninoMeta metaCopy = new JaninoMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        JaninoMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validate(metaCopy);
  }

  private static void validate(JaninoMeta meta) {
    assertEquals(3, meta.getFunctions().size());
    JaninoMetaFunction f = meta.getFunctions().getFirst();
    assertEquals("f1", f.getFieldName());
    assertEquals("expression1", f.getFormula());
    assertEquals(IValueMeta.TYPE_STRING, f.getValueType());
    assertEquals(100, f.getValueLength());
    assertEquals(-1, f.getValuePrecision());
    assertEquals("replace1", f.getReplaceField());

    f = meta.getFunctions().get(1);
    assertEquals("f2", f.getFieldName());
    assertEquals("expression2", f.getFormula());
    assertEquals(IValueMeta.TYPE_INTEGER, f.getValueType());
    assertEquals(7, f.getValueLength());
    assertEquals(-1, f.getValuePrecision());
    assertEquals("replace2", f.getReplaceField());

    f = meta.getFunctions().get(2);
    assertEquals("f3", f.getFieldName());
    assertEquals("expression3", f.getFormula());
    assertEquals(IValueMeta.TYPE_NUMBER, f.getValueType());
    assertEquals(9, f.getValueLength());
    assertEquals(2, f.getValuePrecision());
    assertEquals("replace3", f.getReplaceField());
  }
}
