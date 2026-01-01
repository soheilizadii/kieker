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

import java.util.concurrent.TimeUnit;

import kieker.analysis.architecture.trace.execution.ExecutionRecordTransformationStage;
import kieker.analysis.architecture.trace.reconstruction.TraceReconstructionStage;
import kieker.analysis.generic.CountingStage;
import kieker.analysis.generic.DynamicEventDispatcher;
import kieker.analysis.generic.IEventMatcher;
import kieker.analysis.generic.ImplementsEventMatcher;
import kieker.analysis.generic.source.rewriter.NoneTraceMetadataRewriter;
import kieker.analysis.generic.source.tcp.MultipleConnectionTcpSourceStage;
import kieker.analysis.stage.kafka.KafkaOperationExecutionRecordConsumerStage;
import kieker.common.record.IMonitoringRecord;
import kieker.common.record.controlflow.OperationExecutionRecord;
import kieker.model.repository.SystemModelRepository;
import kieker.tools.oteltransformer.stages.OpenTelemetryExporterStage;
import kieker.tools.oteltransformer.stages.OperationExecutionRecordMatchedStage;
import kieker.tools.oteltransformer.stages.RecordReceiveMetricStage;
import teetime.framework.Configuration;

/**
 * A configuration for exporting Kieker data to OpenTelemetry.
 *
 * Supports two input modes:
 *  - TCP input (default): TeaStore -> transformer TCP source
 *  - Kafka input: Kafka -> transformer consumer group (partition->pod affinity)
 *
 * @author DaGeRe
 */
public class OpenTelemetryExportConfiguration extends Configuration {

	/**
	 * TCP input mode constructor (original behaviour).
	 */
	public OpenTelemetryExportConfiguration(final int inputPort,
											final int bufferSize,
											final kieker.common.configuration.Configuration configuration) {

		final MultipleConnectionTcpSourceStage source =
				new MultipleConnectionTcpSourceStage(inputPort, bufferSize, new NoneTraceMetadataRewriter());

		buildPipeline(source.getOutputPort(), configuration);
	}

	/**
	 * Kafka input mode constructor.
	 *
	 * @param bootstrapServers Kafka bootstrap servers, e.g. "kafka:9092"
	 * @param topic            topic name, e.g. "kieker-records"
	 * @param groupId          consumer group id (MUST be the same for all transformer pods)
	 */
	public OpenTelemetryExportConfiguration(final String bootstrapServers,
											final String topic,
											final String groupId,
											final kieker.common.configuration.Configuration configuration) {

		final KafkaOperationExecutionRecordConsumerStage source =
				new KafkaOperationExecutionRecordConsumerStage(bootstrapServers, topic, groupId);

		buildPipeline(source.getOutputPort(), configuration);
	}

	/**
	 * Common pipeline after the source stage:
	 * source -> counter -> metrics -> dispatcher -> match -> transform -> reconstruct -> OTEL export
	 */
	private void buildPipeline(final teetime.framework.OutputPort<IMonitoringRecord> sourceOutputPort,
							   final kieker.common.configuration.Configuration configuration) {

		final CountingStage<IMonitoringRecord> counter = new CountingStage<>(true, 1000);
		connectPorts(sourceOutputPort, counter.getInputPort());

		final DynamicEventDispatcher dispatcher = new DynamicEventDispatcher(null, false, true, false);
		final IEventMatcher<? extends OperationExecutionRecord> operationExecutionRecordMatcher =
				new ImplementsEventMatcher<>(OperationExecutionRecord.class, null);
		dispatcher.registerOutput(operationExecutionRecordMatcher);

		final RecordReceiveMetricStage recordMetricStage = new RecordReceiveMetricStage();
		connectPorts(counter.getRelayedEventsOutputPort(), recordMetricStage.getInputPort());
		connectPorts(recordMetricStage.getOutputPort(), dispatcher.getInputPort());

		final SystemModelRepository repository = new SystemModelRepository();

		final OperationExecutionRecordMatchedStage matchedStage = new OperationExecutionRecordMatchedStage();
		connectPorts(operationExecutionRecordMatcher.getOutputPort(), matchedStage.getInputPort());

		final ExecutionRecordTransformationStage executionRecordTransformationStage =
				new ExecutionRecordTransformationStage(repository);
		connectPorts(matchedStage.getOutputPort(), executionRecordTransformationStage.getInputPort());

		final TraceReconstructionStage traceReconstructionStage =
				new TraceReconstructionStage(repository, TimeUnit.MILLISECONDS, true, 10000L);
		connectPorts(executionRecordTransformationStage.getOutputPort(), traceReconstructionStage.getInputPort());

		final OpenTelemetryExporterStage otstage = new OpenTelemetryExporterStage(configuration);
		connectPorts(traceReconstructionStage.getExecutionTraceOutputPort(), otstage.getInputPort());
	}
}
