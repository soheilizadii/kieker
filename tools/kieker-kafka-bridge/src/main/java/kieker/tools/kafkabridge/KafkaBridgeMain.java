package kieker.tools.kafkabridge;

import java.nio.file.Path;

import com.beust.jcommander.JCommander;

import kieker.common.configuration.Configuration;
import kieker.common.exception.ConfigurationException;
import kieker.monitoring.core.configuration.ConfigurationFactory;
import kieker.tools.common.AbstractService;

public final class KafkaBridgeMain extends AbstractService<KafkaBridgeConfiguration, Settings> {

    private final Settings parameter = new Settings();

    private KafkaBridgeMain() {}

    public static void main(final String[] args) {
        final KafkaBridgeMain main = new KafkaBridgeMain();
        System.exit(main.run("Kieker Kafka Bridge", "kieker-kafka-bridge", args));
    }

    public int run(final String title, final String label, final String[] args) {
        return super.run(title, label, args, this.parameter);
    }

    @Override
    protected KafkaBridgeConfiguration createTeetimeConfiguration() throws ConfigurationException {
        final Configuration configuration;
        if (parameter.getKiekerMonitoringProperties() != null) {
            configuration = ConfigurationFactory.createConfigurationFromFile(parameter.getKiekerMonitoringProperties());
        } else {
            configuration = ConfigurationFactory.createDefaultConfiguration();
        }

        return new KafkaBridgeConfiguration(
                parameter.getListenPort(),
                parameter.getBufferSize(),
                parameter.getBootstrapServers(),
                parameter.getTopic(),
                parameter.getLogEvery(),
                configuration);
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
        return true;
    }
    @Override
    protected void shutdownService() {
        // nothing to do
    }
}
