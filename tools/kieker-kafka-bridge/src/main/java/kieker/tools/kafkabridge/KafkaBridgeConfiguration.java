package kieker.tools.kafkabridge;

import kieker.analysis.generic.DynamicEventDispatcher;
import kieker.analysis.generic.IEventMatcher;
import kieker.analysis.generic.ImplementsEventMatcher;
import kieker.analysis.generic.source.rewriter.NoneTraceMetadataRewriter;
import kieker.analysis.generic.source.tcp.MultipleConnectionTcpSourceStage;
import kieker.common.record.controlflow.OperationExecutionRecord;
import kieker.tools.kafkabridge.stages.KafkaOperationExecutionRecordProducerStage;
import teetime.framework.Configuration;

public class KafkaBridgeConfiguration extends Configuration {

    public KafkaBridgeConfiguration(
            final int inputPort,
            final int bufferSize,
            final String bootstrapServers,
            final String topic,
            final int logEvery,
            final kieker.common.configuration.Configuration configuration) {

        // configuration currently unused, intentionally kept to match Kieker tool style

        final MultipleConnectionTcpSourceStage source =
                new MultipleConnectionTcpSourceStage(inputPort, bufferSize, new NoneTraceMetadataRewriter());

        final DynamicEventDispatcher dispatcher = new DynamicEventDispatcher(null, false, true, false);

        final IEventMatcher<? extends OperationExecutionRecord> matcher =
                new ImplementsEventMatcher<>(OperationExecutionRecord.class, null);

        dispatcher.registerOutput(matcher);

        connectPorts(source.getOutputPort(), dispatcher.getInputPort());

        final KafkaOperationExecutionRecordProducerStage kafka =
                new KafkaOperationExecutionRecordProducerStage(bootstrapServers, topic, logEvery);

        // The matcher output port is already OperationExecutionRecord typed
        connectPorts(matcher.getOutputPort(), kafka.getInputPort());
    }
}
