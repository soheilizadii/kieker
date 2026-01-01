/***************************************************************************
 * Copyright (C) 2024 Kieker Project (https://kieker-monitoring.net)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************/
package kieker.tools.oteltransformer;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;

import com.beust.jcommander.JCommander;
import com.sun.net.httpserver.HttpServer;

import kieker.common.configuration.Configuration;
import kieker.common.exception.ConfigurationException;
import kieker.monitoring.core.configuration.ConfigurationFactory;
import kieker.tools.common.AbstractService;
import kieker.tools.oteltransformer.metrics.SimpleMetrics;

/**
 * Receives Kieker records, to later on pass them to OpenTelemetry (like the MooBench record receiver).
 *
 * @author David Georg Reichelt, Reiner Jung
 */
public final class RecordReceiverMain extends AbstractService<OpenTelemetryExportConfiguration, Settings> {

	private final Settings parameter = new Settings();

	private HttpServer metricsServer;

	private RecordReceiverMain() {
	}

	public static void main(final String[] args) {
		final RecordReceiverMain main = new RecordReceiverMain();
		System.exit(main.run("OpenTelemetry Transformer", "transformer", args));
	}

	public int run(final String title, final String label, final String[] args) {
		startMetricsServer();
		return super.run(title, label, args, this.parameter);
	}

	@Override
	protected OpenTelemetryExportConfiguration createTeetimeConfiguration() throws ConfigurationException {
		final Configuration configuration;

		if (parameter.getKiekerMonitoringProperties() != null) {
			configuration = ConfigurationFactory.createConfigurationFromFile(parameter.getKiekerMonitoringProperties());
		} else {
			configuration = ConfigurationFactory.createDefaultConfiguration();
		}

		if (parameter.isKafkaInput()) {
			System.out.println("Starting OTEL Transformer with Kafka input");
			return new OpenTelemetryExportConfiguration(
					parameter.getKafkaBootstrapServers(),
					parameter.getKafkaTopic(),
					parameter.getKafkaGroupId(),
					configuration
			);
		}

		System.out.println("Starting OTEL Transformer with TCP input on port " + parameter.getListenPort());
		return new OpenTelemetryExportConfiguration(
				parameter.getListenPort(),
				8192,
				configuration
		);
	}

	@Override
	protected Path getConfigurationPath() {
		return this.parameter.getKiekerMonitoringProperties();
	}

	@Override
	protected boolean checkConfiguration(final Configuration configuration, final JCommander commander) {
		return true;
	}

	@Override
	protected boolean checkParameters(final JCommander commander) throws ConfigurationException {
		this.parameter.validate();
		return true;
	}


	private void startMetricsServer() {
		try {
			int port = 9464; // default
			final String env = System.getenv("OTEL_TRANSFORMER_METRICS_PORT");
			if (env != null && !env.isBlank()) {
				port = Integer.parseInt(env.trim());
			}

			metricsServer = HttpServer.create(new InetSocketAddress(port), 0);
			metricsServer.createContext("/metrics", exchange -> {
				final byte[] body = SimpleMetrics.renderPrometheusBytes();
				exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
				exchange.sendResponseHeaders(200, body.length);
				try (OutputStream os = exchange.getResponseBody()) {
					os.write(body);
				}
			});

			metricsServer.setExecutor(null); // default executor
			metricsServer.start();
			System.out.println("Metrics server started on : " + port + " (GET /metrics)");
		} catch (Exception e) {
			System.err.println("Failed to start metrics server: " + e.getMessage());
		}
	}

	private void stopMetricsServer() {
		if (metricsServer != null) {
			metricsServer.stop(0);
			metricsServer = null;
		}
	}

	@Override
	protected void shutdownService() {
		stopMetricsServer();
	}
}
