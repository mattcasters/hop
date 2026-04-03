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

package org.apache.hop.py4j;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** This is the starting point for the Hop Python scripting methods. In here we'll have */
@Getter
@Setter
public class PyHop {
  private IVariables variables;
  private IHopMetadataProvider metadataProvider;
  private ILogChannel log;

  public PyHop() {}

  public void initialize(
      IVariables variables, IHopMetadataProvider metadataProvider, ILogChannel log) {
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = log;
  }

  public ILogChannel getLogChannel() {
    return log;
  }

  public PipelineMeta newPipeline() {
    return new PipelineMeta();
  }

  public TransformMeta newTransform(String name, String pluginId) throws HopPluginException {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName(name);

    PluginRegistry registry = PluginRegistry.getInstance();
    ITransformMeta transform =
        registry.loadClass(TransformPluginType.class, pluginId, ITransformMeta.class);
    if (transform == null) {
      throw new HopPluginException("Unable to find transform plugin ID: " + pluginId);
    }
    transformMeta.setTransform(transform);
    return transformMeta;
  }

  public PipelineHopMeta newHop(TransformMeta from, TransformMeta to) {
    return new PipelineHopMeta(from, to);
  }

  public void print(PipelineMeta pipelineMeta) throws HopException {
    log.logBasic(pipelineMeta.getName() + " : " + pipelineMeta.getXml(variables));
  }
}
