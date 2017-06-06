package im;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import im.utils.GenericAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yuchaoma on 14/04/2017.
 */

// TODO log error messages
// TODO how to test these functions


public class GroupOperationConverter {

    Properties properties;
    public GroupOperationConverter(Properties properties){
        this.properties = properties;
    }


    public void run() throws IOException {
        Properties streamsConfiguration = new Properties();
        //"cdp-bj1-kafka-sr1:9092"
        String bootstrapServers = this.properties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //"http://cdp-bj1-kafka-sr2:8081"
        String schemaRegistryUrl = "http://" +  this.properties.getProperty("schema.registry.url", "localhost:8081");
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "ETL for IM Operation Message");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put("schema.registry.url", schemaRegistryUrl);

        String outTopic =properties.getProperty("outputTopic", "im_group_operations_cdp");


        final Schema schema = new Schema.Parser().parse(new File("src/main/java/im/group_operation.avro"));

        boolean isKeySerde = false;
        GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
        genericAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
                isKeySerde);


        final Serde<String> stringSerde = Serdes.String();

        KStreamBuilder builder = new KStreamBuilder();

        String inputTopic = properties.getProperty("inputTopic");
        KStream<String, String> inStream = builder.stream(stringSerde, stringSerde, inputTopic);

        KStream<String, GenericRecord> outStream =inStream.mapValues(value -> {
            GenericRecord msg = null;
            //TODO how to handle this exception
            try {
                msg = this.convertToCdpAvro(value, schema);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return msg;
        } ).filter((key,value) -> value != null);
//            KStream<String, String> outStream =inStream.mapValues(value -> value);
        outStream.to(stringSerde, genericAvroSerde , outTopic);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public GenericRecord convertToCdpAvro(String recordStr , Schema schema) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode source = mapper.readTree(recordStr);
        GenericRecord msg = new GenericData.Record(schema);
        msg.put("group_id", source.get("groupid").asText());
        msg.put("appkey", source.get("appkey").asText());
        msg.put("action", source.get("event_type").asText());
        msg.put("operator", source.get("invoker").asText());
        msg.put("group_type", source.get("group_type").asText());

        //TODO refactor
        if(source.get("reason") != null){
            msg.put("reason", source.get("reason").asText());
        }
        if(source.get("mode") != null){
            msg.put("mode", source.get("mode").asText());
        }
        if(source.get("join_type") != null) {
            msg.put("join_type", source.get("join_type").asText());
        }
        if(source.get("member") != null) {
            msg.put("member", source.get("member").asText());
        }
        if(source.get("from") != null) {
            msg.put("from", source.get("from").asText());
        }
        if(source.get("to") != null) {
            msg.put("to", source.get("to").asText());
        }
        if(source.get("title") != null) {
            msg.put("group_name", source.get("title").asText());
        }

        String[] settingFields = {"role","mute_duration", "slice_size", "description", "scale", "members_only" ,
                "type","max_users","slice_num","invite_need_confirm","public","allow_user_invites",
                "last_modified","owner","created" ,"custom"    ,"debut_msg_num"};
        for(String field: settingFields){
            HashMap<String, String> settings = new HashMap<>();
            if(source.get(field) != null){
                settings.put(field, source.get(field).asText());
            }
            msg.put("settings", settings);
        }

        String ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(new Date(source.get("timestamp").asLong()));
        msg.put("timestamp", ts);
        return msg;
    }
}
