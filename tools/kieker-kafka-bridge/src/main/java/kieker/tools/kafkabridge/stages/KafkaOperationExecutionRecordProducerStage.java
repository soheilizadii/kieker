package kieker.tools.kafkabridge.stages;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import kieker.common.record.controlflow.OperationExecutionRecord;
import teetime.framework.AbstractConsumerStage;

public class KafkaOperationExecutionRecordProducerStage extends AbstractConsumerStage<OperationExecutionRecord> {

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;
    private final int logEvery;
    private long count;

    public KafkaOperationExecutionRecordProducerStage(final String bootstrapServers, final String topic, final int logEvery) {
        this.topic = topic;
        this.logEvery = logEvery;

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        this.producer = new KafkaProducer<>(props);
    }

    @Override
    protected void execute(final OperationExecutionRecord r) {
        final String key = Long.toString(r.getTraceId()); // partition by traceId
        final byte[] value = toJsonBytes(r);
        producer.send(new ProducerRecord<>(topic, key, value));

        count++;
        if (logEvery > 0 && count % logEvery == 0) {
            System.out.println("Produced OER #" + count
                    + " traceId=" + r.getTraceId()
                    + " eoi=" + r.getEoi()
                    + " ess=" + r.getEss());
        }
    }

    @Override
    protected void onTerminating() {
        try {
            producer.flush();
            producer.close(Duration.ofSeconds(10));
        } catch (final Exception ignored) {
        }
    }

    private static byte[] toJsonBytes(final OperationExecutionRecord r) {
        final String json = "{"
                + "\"traceId\":" + r.getTraceId() + ","
                + "\"eoi\":" + r.getEoi() + ","
                + "\"ess\":" + r.getEss() + ","
                + "\"tin\":" + r.getTin() + ","
                + "\"tout\":" + r.getTout() + ","
                + "\"sessionId\":\"" + esc(r.getSessionId()) + "\","
                + "\"hostname\":\"" + esc(r.getHostname()) + "\","
                + "\"operationSignature\":\"" + esc(r.getOperationSignature()) + "\""
                + "}";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    private static String esc(final String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
}
