package kieker.tools.oteltransformer;

import java.nio.file.Path;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.PathConverter;

public class Settings {

	@Parameter(
			names = { "-lp", "--listenPort" },
			required = false,
			description = "Port where the otel-transformer listens for traces"
	)
	private int listenPort = 0;

	@Parameter(
			names = { "-c", "--configuration" },
			required = false,
			description = "Configuration file.",
			converter = PathConverter.class
	)
	private Path configurationPath;

	public int getListenPort() {
		return listenPort;
	}

	public Path getKiekerMonitoringProperties() {
		return this.configurationPath;
	}

	public boolean isKafkaInput() {
		return "kafka".equalsIgnoreCase(
				System.getenv().getOrDefault("INPUT_MODE", "tcp")
		);
	}

	public String getKafkaBootstrapServers() {
		return System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
	}

	public String getKafkaTopic() {
		return System.getenv().getOrDefault("KAFKA_TOPIC", "kieker-records");
	}

	public String getKafkaGroupId() {
		return System.getenv().getOrDefault("KAFKA_GROUP_ID", "otel-transformer");
	}

	/** Call after args parsing. */
	public void validate() {
		if (!isKafkaInput() && listenPort <= 0) {
			throw new ParameterException(
					"In TCP mode you must provide -lp/--listenPort with a value > 0."
			);
		}
	}
}
