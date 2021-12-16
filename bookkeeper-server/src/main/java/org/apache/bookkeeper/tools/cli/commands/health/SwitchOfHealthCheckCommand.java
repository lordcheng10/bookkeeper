/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.tools.cli.commands.health;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.meta.zk.ZKMetadataBookieDriver;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;


/**
 * Command to enable or disable auto recovery in the cluster.
 */
public class SwitchOfHealthCheckCommand extends BookieCommand<SwitchOfHealthCheckCommand.HealthCheckFlags> {

    static final Logger LOG = LoggerFactory.getLogger(SwitchOfHealthCheckCommand.class);

    private static final String NAME = "switch";
    private static final String DESC = "Enable or disable health check in the cluster. Default is enable.";

    public SwitchOfHealthCheckCommand() {
        this(new HealthCheckFlags());
    }

    private SwitchOfHealthCheckCommand(HealthCheckFlags flags) {
        super(CliSpec.<HealthCheckFlags>newBuilder()
                .withName(NAME).withDescription(DESC)
                .withFlags(flags).build());
    }

    /**
     * Flags for health check command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class HealthCheckFlags extends CliFlags {

        @Parameter(names = { "-e", "--enable" }, description = "Enable or disable health check.")
        private boolean enable;

        @Parameter(names = {"-s", "--status"}, description = "Check the health check status.")
        private boolean status;

    }

    @Override
    public boolean apply(ServerConfiguration conf, HealthCheckFlags cmdFlags) {
        try {
            return handler(conf, cmdFlags);
        } catch (MetadataException | ExecutionException e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean handler(ServerConfiguration conf, HealthCheckFlags flags)
            throws MetadataException, ExecutionException {

        MetadataDrivers.runFunctionWithMetadataBookieDriver(conf, driver -> {
            try {
                String enableHealthPath = conf.getEnableHealthPath();

                if (flags.status) {
                    LOG.info("EnableHealthCheck is " + (driver.isEnableHealthCheck()
                            ? "enabled." : "disabled."));
                    return null;
                }

                if (flags.enable) {
                    if (driver.isEnableHealthCheck()) {
                        LOG.warn("HealthCheck already enable. Doing nothing");
                    } else {
                        LOG.info("Enable HealthCheck");
                        driver.enableHealthCheck(enableHealthPath);
                    }
                } else {
                    if (!driver.isEnableHealthCheck()) {
                        LOG.warn("HealthCheck already disable. Doing nothing");
                    } else {
                        LOG.info("Disable HealthCheck");
                        driver.disableHealthCheck(enableHealthPath);
                    }
                }
            } catch (Exception e) {
                LOG.error("exception", e);
                throw new UncheckedExecutionException(e);
            }
            return null;
        });
        return true;
    }
}
