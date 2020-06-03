/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.launchers.remotefs;

import io.activej.async.service.EventloopTaskScheduler;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.di.annotation.Inject;
import io.activej.di.annotation.Named;
import io.activej.di.annotation.Optional;
import io.activej.di.annotation.Provides;
import io.activej.di.module.Module;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.ThrottlingController;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.remotefs.*;
import io.activej.service.ServiceGraphModule;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import static io.activej.common.Utils.nullToDefault;
import static io.activej.config.ConfigConverters.ofPath;
import static io.activej.di.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;
import static io.activej.launchers.initializers.Initializers.ofEventloopTaskScheduler;
import static io.activej.launchers.remotefs.Initializers.*;
import static io.activej.remotefs.ServerSelector.RENDEZVOUS_HASH_SHARDER;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public abstract class RemoteFsClusterLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "remotefs-cluster.properties";

	@Inject
	RemoteFsRepartitionController controller;

	@Inject
	@Named("repartition")
	EventloopTaskScheduler repartitionScheduler;

	@Inject
	@Named("clusterDeadCheck")
	EventloopTaskScheduler clusterDeadCheckScheduler;

	@Provides
	Eventloop eventloop(Config config, @Optional ThrottlingController throttlingController) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.initialize(eventloop -> eventloop.withInspector(throttlingController));
	}

	@Provides
	@Named("repartition")
	EventloopTaskScheduler eventloopTaskScheduler(Config config, Eventloop eventloop, RemoteFsRepartitionController controller1) {
		return EventloopTaskScheduler.create(eventloop, controller1::repartition)
				.initialize(ofEventloopTaskScheduler(config.getChild("scheduler.repartition")));
	}

	@Provides
	@Named("clusterDeadCheck")
	EventloopTaskScheduler deadCheckScheduler(Config config, Eventloop eventloop, RemoteFsClusterClient cluster) {
		return EventloopTaskScheduler.create(eventloop, cluster::checkDeadPartitions)
				.initialize(ofEventloopTaskScheduler(config.getChild("scheduler.cluster.deadCheck")));
	}

	@Provides
	RemoteFsRepartitionController repartitionController(Config config,
			RemoteFsServer localServer, RemoteFsClusterClient cluster) {
		return RemoteFsRepartitionController.create(config.get("remotefs.repartition.localPartitionId"), cluster)
				.initialize(ofRepartitionController(config.getChild("remotefs.repartition")));
	}

	@Provides
	RemoteFsClusterClient remoteFsClusterClient(Config config,
			RemoteFsServer localServer, Eventloop eventloop,
			@Optional ServerSelector serverSelector) {
		Map<Object, FsClient> clients = new HashMap<>();
		clients.put(config.get("remotefs.repartition.localPartitionId"), localServer.getClient());
		return RemoteFsClusterClient.create(eventloop, clients)
				.withServerSelector(nullToDefault(serverSelector, RENDEZVOUS_HASH_SHARDER))
				.initialize(ofRemoteFsCluster(eventloop, config.getChild("remotefs.cluster")));
	}

	@Provides
	RemoteFsServer remoteFsServer(Config config, Eventloop eventloop, Executor executor) {
		return RemoteFsServer.create(eventloop, executor, config.get(ofPath(), "remotefs.server.path"))
				.initialize(ofRemoteFsServer(config.getChild("remotefs.server")));
	}

	@Provides
	public Executor executor() {
		return newSingleThreadExecutor();
	}

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(Config.ofProperties(PROPERTIES_FILE, true))
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RemoteFsClusterLauncher() {};
		launcher.launch(args);
	}
}