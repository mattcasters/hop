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

import static java.lang.Thread.sleep;

import java.net.InetAddress;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.hop.Hop;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import picocli.CommandLine;
import py4j.GatewayServer;

@Getter
@Setter
@CommandLine.Command(
    mixinStandardHelpOptions = true,
    name = "python",
    description = "Run the Hop Python gateway (py4j)")
@HopCommand(id = "python", description = "Run the Hop Python gateway")
public class PythonCommand implements Runnable, IHopCommand, IHasHopMetadataProvider {
  public static final String CATEGORY_PYTHON = "python";

  private ILogChannel log;
  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;

  @CommandLine.Option(
      names = {"--gateway-port"},
      description =
          "The port on which to run the Hop Python (py4j) gateway service.  The default port is 25333.")
  private String gatewayPort;

  @CommandLine.Option(
      names = {"--gateway-ip-address"},
      description =
          "The server on which to run the Hop Python (py4j) gateway service.  The default is 127.0.0.1 (localhost).  Use 0.0.0.0 to make the service widely available.")
  private String gatewayAddress;

  private PyHop pyHop;

  public PythonCommand() {}

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = new LogChannel("HopPython");

    pyHop = new PyHop();
    pyHop.initialize(variables, metadataProvider, log);

    // Same plugins as for RUN,DOC, etc. It's mainly for loading projects etc.
    Hop.addMixinPlugins(cmd, ConfigPlugin.CATEGORY_PYTHON);
  }

  /**
   * Sets metadataProvider
   *
   * @param metadataProvider value of metadataProvider
   */
  public void setMetadataProvider(MultiMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
    this.pyHop.setMetadataProvider(metadataProvider);
  }

  /**
   * Sets variables
   *
   * @param variables value of variables
   */
  public void setVariables(IVariables variables) {
    this.variables = variables;
    this.pyHop.setVariables(variables);
  }

  protected void handleMixinActions() throws HopException {
    // Handle the options of the configuration plugins
    //
    Map<String, Object> mixins = cmd.getMixins();
    for (Map.Entry<String, Object> entry : mixins.entrySet()) {
      Object mixin = entry.getValue();
      if (mixin instanceof IConfigOptions configOptions) {
        configOptions.handleOption(log, this, variables);
      }
    }
  }

  @Override
  public void run() {
    // Check a few variables...
    //
    try {
      System.setProperty(Const.HOP_PLATFORM_RUNTIME, "PYTHON");
      handleMixinActions();

      int port = Const.toInt(variables.resolve(gatewayPort), 25333);
      String ipAddress = variables.resolve(gatewayAddress);
      if (StringUtils.isEmpty(ipAddress)) {
        ipAddress = "127.0.0.1";
      }

      // Run the gateway
      //
      GatewayServer.GatewayServerBuilder builder = new GatewayServer.GatewayServerBuilder();
      GatewayServer gatewayServer =
          builder
              .entryPoint(this)
              .javaPort(port)
              .javaAddress(InetAddress.getByName(ipAddress))
              .build();
      gatewayServer.start();
      log.logBasic("The Hop Python Gateway server was started on " + ipAddress + ":" + port);

      do sleep(100);
      while (true);
    } catch (Exception e) {
      log.logError("Error running the Hop Python Gateway server (py4j)", e);
    }
  }
}
