package kieker.tools.kafkabridge;

import java.nio.file.Path;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.PathConverter;

public class Settings {

    @Parameter(names = {"-lp", "--listenPort"}, required = true,
            description = "Port where the bridge listens for incoming Kieker TCP records")
    private int listenPort;

    @Parameter(names = {"-bs", "--bootstrapServers"}, required = true,
            description = "Kafka bootstrap.servers, e.g. kafka:9092")
    private String bootstrapServers;

    @Parameter(names = {"-t", "--topic"}, required = false,
            description = "Kafka topic name")
    private String topic = "kieker-records";

    @Parameter(names = {"--bufferSize"}, required = false,
            description = "TCP buffer size for the Kieker TCP source stage")
    private int bufferSize = 8192;

    @Parameter(names = {"--logEvery"}, required = false,
            description = "Log every N produced records (0 disables)")
    private int logEvery = 1000;

    @Parameter(names = {"-c", "--configuration"}, required = false,
            description = "Configuration file.", converter = PathConverter.class)
    private Path configurationPath;

    public int getListenPort() { return listenPort; }
    public String getBootstrapServers() { return bootstrapServers; }
    public String getTopic() { return topic; }
    public int getBufferSize() { return bufferSize; }
    public int getLogEvery() { return logEvery; }

    // Keep same naming convention as otel-transformer
    public Path getKiekerMonitoringProperties() { return configurationPath; }
}
