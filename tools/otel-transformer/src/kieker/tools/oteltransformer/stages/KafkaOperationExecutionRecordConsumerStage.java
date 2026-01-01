package kieker.analysis.stage.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import kieker.common.record.IMonitoringRecord;
import kieker.common.record.controlflow.OperationExecutionRecord;
import teetime.framework.AbstractProducerStage;

/**
 * Reads OperationExecutionRecords from Kafka (value = JSON bytes) and emits them as IMonitoringRecord.
 *
 * IMPORTANT:
 * - All transformer pods must use the SAME group.id (consumer group).
 * - Kafka keys must be trace-affinity keys (bridge sets key=traceId) so that
 *   all records of the same trace go to the same partition and therefore to the same consumer instance.
 */
public class KafkaOperationExecutionRecordConsumerStage extends AbstractProducerStage<IMonitoringRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOperationExecutionRecordConsumerStage.class);

    /**
     * DTO used for JSON decoding. Kieker's OperationExecutionRecord does not expose
     * a Jackson-friendly default constructor / @JsonCreator, so we map JSON into this DTO
     * and then construct the Kieker record explicitly.
     */
    static final class OerDto {
        public long traceId;
        public int eoi;
        public int ess;
        public long tin;
        public long tout;
        public String sessionId;
        public String hostname;
        public String operationSignature;
    }

    private final KafkaConsumer<String, byte[]> consumer;
    private final ObjectMapper mapper;
    private volatile boolean running = true;

    public KafkaOperationExecutionRecordConsumerStage(final String bootstrapServers,
                                                      final String topic,
                                                      final String groupId) {

        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);

        // Simple/robust defaults
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "latest");
        props.put("max.poll.records", "500");

        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());

        // Unique client id helps debugging
        props.put("client.id", "otel-transformer-" + UUID.randomUUID());

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));

        // Configure JSON mapper
        this.mapper = new ObjectMapper()
                // be resilient to extra fields / schema evolution
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void execute() {
        try {
            while (running) {
                final ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(250));

                for (ConsumerRecord<String, byte[]> rec : records) {
                    try {
                        final OerDto dto = mapper.readValue(rec.value(), OerDto.class);

                        // Build the Kieker record explicitly (no Jackson constructor available)
                        final OperationExecutionRecord oer = new OperationExecutionRecord(
                                dto.operationSignature,
                                dto.sessionId,
                                dto.traceId,
                                dto.tin,
                                dto.tout,
                                dto.hostname,
                                dto.eoi,
                                dto.ess
                        );

                        // Emit to next stage
                        this.outputPort.send(oer);

                    } catch (Exception e) {
                        LOG.error("Failed to decode Kafka record (topic={}, partition={}, offset={})",
                                rec.topic(), rec.partition(), rec.offset(), e);
                    }
                }
            }
        } catch (WakeupException e) {
            // Expected during shutdown when wakeup() is called
            if (running) {
                throw e;
            }
        } finally {
            try {
                consumer.close();
            } catch (Exception e) {
                LOG.warn("Error while closing Kafka consumer", e);
            }
        }
    }

    @Override
    public void onTerminating() {
        running = false;
        try {
            consumer.wakeup(); // unblock poll()
        } catch (Exception ignored) {
            // ignore
        }
    }
}
