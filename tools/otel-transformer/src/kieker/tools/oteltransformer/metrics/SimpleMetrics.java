package kieker.tools.oteltransformer.metrics;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exposes counters in Prometheus text exposition format.
 */
public final class SimpleMetrics {

    private SimpleMetrics() {}

    // Counters
    private static final AtomicLong TRACES_PROCESSED_TOTAL = new AtomicLong(0);
    private static final AtomicLong SPANS_TRANSFORMED_TOTAL = new AtomicLong(0);

    private static final AtomicLong RECORDS_RECEIVED_TOTAL = new AtomicLong(0);

    private static final AtomicLong OPERATION_EXECUTION_RECORDS_MATCHED_TOTAL = new AtomicLong(0);


    private static final AtomicLong OPERATION_EXECUTION_RECORDS_RECEIVED_TOTAL = new AtomicLong(0);


    public static void incTracesProcessed() {
        TRACES_PROCESSED_TOTAL.incrementAndGet();
    }

    public static void incSpansTransformed() {
        SPANS_TRANSFORMED_TOTAL.incrementAndGet();
    }

    public static long getTracesProcessedTotal() {
        return TRACES_PROCESSED_TOTAL.get();
    }

    public static long getSpansTransformedTotal() {return SPANS_TRANSFORMED_TOTAL.get();}


    public static void incRecordsReceived() {RECORDS_RECEIVED_TOTAL.incrementAndGet();}

    public static long getRecordsReceivedTotal() {return RECORDS_RECEIVED_TOTAL.get();}

    public static void incOperationExecutionRecordsReceived() {OPERATION_EXECUTION_RECORDS_RECEIVED_TOTAL.incrementAndGet();}

    public static long getOperationExecutionRecordsReceivedTotal() {return OPERATION_EXECUTION_RECORDS_RECEIVED_TOTAL.get();}

    public static void incOperationExecutionRecordsMatched() {OPERATION_EXECUTION_RECORDS_MATCHED_TOTAL.incrementAndGet();}

    public static long getOperationExecutionRecordsMatchedTotal() {return OPERATION_EXECUTION_RECORDS_MATCHED_TOTAL.get();}


    /**
     * Prometheus text exposition format (version 0.0.4).
     */
    public static String renderPrometheusText() {
        StringBuilder sb = new StringBuilder(256);

        sb.append("# HELP k2otel_traces_processed_total Total number of Kieker ExecutionTraces processed by the transformer.\n");
        sb.append("# TYPE k2otel_traces_processed_total counter\n");
        sb.append("k2otel_traces_processed_total ").append(getTracesProcessedTotal()).append('\n');

        sb.append("# HELP k2otel_spans_transformed_total Total number of OpenTelemetry spans created (transformed) from Kieker Executions.\n");
        sb.append("# TYPE k2otel_spans_transformed_total counter\n");
        sb.append("k2otel_spans_transformed_total ").append(getSpansTransformedTotal()).append('\n');

        sb.append("# HELP k2otel_records_received_total Total number of Kieker monitoring records received by the transformer.\n");
        sb.append("# TYPE k2otel_records_received_total counter\n");
        sb.append("k2otel_records_received_total ").append(getRecordsReceivedTotal()).append('\n');

        sb.append("# HELP k2otel_operation_execution_records_received_total Total number of OperationExecutionRecord received by the transformer.\n");
        sb.append("# TYPE k2otel_operation_execution_records_received_total counter\n");
        sb.append("k2otel_operation_execution_records_received_total ").append(getOperationExecutionRecordsReceivedTotal()).append('\n');

        sb.append("# HELP k2otel_operation_execution_records_matched_total Total number of OperationExecutionRecord routed by the dispatcher (matched) into the pipeline.\n");
        sb.append("# TYPE k2otel_operation_execution_records_matched_total counter\n");
        sb.append("k2otel_operation_execution_records_matched_total ").append(getOperationExecutionRecordsMatchedTotal()).append('\n');

        return sb.toString();
    }

    public static byte[] renderPrometheusBytes() {
        return renderPrometheusText().getBytes(StandardCharsets.UTF_8);
    }
}
