import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Properties;

public class MyKStreamSample {
    private static final Logger logger = LogManager.getLogger();
    private static final String topicName = "yoshio-kafka";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "HelloStreams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "yoshio-kafka-eventhub-ns.servicebus.windows.net:9093");
        props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://yoshio-kafka-eventhub-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=*******************/**/***+***********/****=\";");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, String> kStream = builder.stream(topicName, Consumed.with(Serdes.Long(), Serdes.String()));

        kStream.foreach((k, v) -> System.out.println("Key = " + k + " Value = " + v));
        // kStream.peek((k, v) -> System.out.println("Key = " + k + " Value = " + v));
        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        logger.info("Starting the stream");
        streams.start();


    }
}
