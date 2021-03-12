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

package org.apache.hop.pipeline.transforms.sort;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class SortRowsMetaInjectionTest extends BaseMetadataInjectionTest<SortRowsMeta> {


  @Before
  public void setup() throws Exception {
    setup( new SortRowsMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "SORT_DIRECTORY", () -> meta.getDirectory() );
    check( "SORT_FILE_PREFIX", () -> meta.getPrefix() );
    check( "SORT_SIZE_ROWS", () -> meta.getSortSize() );
    check( "FREE_MEMORY_TRESHOLD", () -> meta.getFreeMemoryLimit() );
    check( "ONLY_PASS_UNIQUE_ROWS", () -> meta.isOnlyPassingUniqueRows() );
    check( "COMPRESS_TEMP_FILES", () -> meta.getCompressFiles() );
    check( "NAME", () -> meta.getFieldName()[ 0 ] );
    check( "SORT_ASCENDING", () -> meta.getAscending()[ 0 ] );
    check( "IGNORE_CASE", () -> meta.getCaseSensitive()[ 0 ] );
    check( "PRESORTED", () -> meta.getPreSortedField()[ 0 ] );
    check( "COLLATOR_STRENGTH", () -> meta.getCollatorStrength()[ 0 ] );
    check( "COLLATOR_ENABLED", () -> meta.getCollatorEnabled()[ 0 ] );
  }
}
