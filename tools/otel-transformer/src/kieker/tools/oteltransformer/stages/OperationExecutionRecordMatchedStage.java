package kieker.tools.oteltransformer.stages;

import kieker.common.record.controlflow.OperationExecutionRecord;
import kieker.tools.oteltransformer.metrics.SimpleMetrics;
import teetime.framework.AbstractConsumerStage;
import teetime.framework.OutputPort;

public final class OperationExecutionRecordMatchedStage extends AbstractConsumerStage<OperationExecutionRecord> {

    private final OutputPort<OperationExecutionRecord> outputPort = this.createOutputPort();

    public OutputPort<OperationExecutionRecord> getOutputPort() {
        return outputPort;
    }

    @Override
    protected void execute(final OperationExecutionRecord record) {
        SimpleMetrics.incOperationExecutionRecordsMatched();
        outputPort.send(record);
    }
}
