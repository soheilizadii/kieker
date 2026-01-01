package kieker.tools.oteltransformer.stages;

import kieker.common.record.IMonitoringRecord;
import kieker.tools.oteltransformer.metrics.SimpleMetrics;
import teetime.framework.AbstractConsumerStage;
import teetime.framework.OutputPort;
import kieker.common.record.controlflow.OperationExecutionRecord;

public final class RecordReceiveMetricStage extends AbstractConsumerStage<IMonitoringRecord> {

    private final OutputPort<IMonitoringRecord> outputPort = this.createOutputPort();

    public OutputPort<IMonitoringRecord> getOutputPort() {
        return outputPort;
    }

    @Override
    protected void execute(final IMonitoringRecord record) {
        SimpleMetrics.incRecordsReceived();

        if (record instanceof OperationExecutionRecord) {
            SimpleMetrics.incOperationExecutionRecordsReceived();
        }

        outputPort.send(record);
    }
}
