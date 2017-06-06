package im;

/**
 * Created by yuchaoma on 01/04/2017.
 */


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import im.utils.GenericAvroSerde;
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
import java.lang.String;

public class ReceiveMessageConverter {

    Properties properties;

    public ReceiveMessageConverter(Properties properties) {
        this.properties = properties;
    }


    public void run() throws IOException {
        Properties streamsConfiguration = new Properties();
        //"cdp-bj1-kafka-sr1:9092"
        String bootstrapServers = this.properties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //"http://cdp-bj1-kafka-sr2:8081"
        String schemaRegistryUrl = "http://" + this.properties.getProperty("schema.registry.url", "localhost:8081");


        String inputTopic = properties.getProperty("inputTopic");
        String applicationId = "ETL for IM Receive Message";
        if (inputTopic.equals("bj-ecs-ol-ejabberd-chat-messages")) {
            applicationId = "ETL for IM Chat Message";
        } else if (inputTopic.equals("bj-ecs-ol-ejabberd-chat-offlines")) {
            applicationId = "ETL for IM Offline Message";
        }

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put("schema.registry.url", schemaRegistryUrl);

        String outTopic = properties.getProperty("outputTopic", "im_receive_msg_cdp");


        final Schema schema = new Schema.Parser().parse(new File("src/main/java/im/msg.avro"));

        boolean isKeySerde = false;
        GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
        genericAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
                isKeySerde);


        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> inStream = builder.stream(stringSerde, stringSerde, inputTopic);

        KStream<String, GenericRecord> outStream = inStream.mapValues(value -> {
            GenericRecord msg = this.convertToCdpAvro(value, schema);
            return msg;
        }).filter((key, value) -> value != null);
//            KStream<String, String> outStream =inStream.mapValues(value -> value);
        outStream.to(stringSerde, genericAvroSerde, outTopic);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    public String getOrgName(String userImContactId) {

        return "";
    }

    public String getAppName(String userImContactId) {

        return "";
    }

    public Integer getPartition(String orgName, String appName) {
        return 1;
    }

    public GenericRecord convertToCdpAvro(String recordStr, Schema schema) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode source = null;
        try {
            source = mapper.readTree(recordStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        GenericRecord msg = new GenericData.Record(schema);
        msg.put("from", source.get("from").asText());
        msg.put("type", "chatmessage");
        msg.put("to", source.get("to").asText());

        String[] includeType = {"chat", "groupchat"};
        if (!Arrays.asList(includeType).contains(source.get("chat_type").asText())) {
            return null;
        }
        // TODO how to get enum work with null compatibly
        msg.put("chat_type", source.get("chat_type").asText());
        msg.put("msg_id", source.get("msg_id").asText());
        String ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(new Date(source.get("timestamp").asLong()));

        String direction = source.get("direction").asText();
        switch (direction) {
            case "incoming":
                msg.put("receive_time", ts);
                msg.put("receiver_absent", false);
                break;
            case "outgoing":
                msg.put("send_time", ts);
                msg.put("receiver_absent", false);
                break;
            case "offline":
                msg.put("send_time", ts);
                msg.put("receiver_absent", true);
                break;
        }

        String orgName = getOrgName(source.get("from").asText());
        String appName = getAppName(source.get("from").asText());
        msg.put("org_name", orgName);
        msg.put("app_name", appName);
        // TODO add partition
        msg.put("partition", getPartition(orgName, appName));

        if (!source.get("payload").isValueNode()) {
            JsonNode payload = source.get("payload");

            JsonNode ext = payload.get("ext");
            if (ext != null) {
                Iterator<String> extFieldNames = payload.get("ext").fieldNames();
                HashMap<String, String> extHash = new HashMap<>();
                while (extFieldNames.hasNext()) {
                    String field = extFieldNames.next();
                    extHash.put(field, ext.get(field).toString());
                }
                msg.put("ext", extHash);
            }
            if (payload.get("from") != null && payload.get("to") != null) {
                msg.put("sdk_from", payload.get("from").asText());
                msg.put("sdk_to", payload.get("to").asText());
            }
            if (((ArrayNode) payload.get("bodies")).size() > 1) {
                try {
                    throw new Exception("I expect bodies size should be 1, but maybe I was wrong!");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            JsonNode msgBody = ((ArrayNode) payload.get("bodies")).get(0);
            // TODO Log messages that are not illegal
            if (msgBody == null || msgBody.get("type") == null) {
                return null;
            }
            String msgType = msgBody.get("type").asText();

            String[] msgTypes = new String[]{"txt", "img", "loc", "audio", "video", "file"};
            if (Arrays.asList(msgTypes).contains(msgType)) {
                msg.put("msg_type", msgType);
                if (msgBody.get("msg") != null) {
                    msg.put("message", msgBody.get("msg").asText());
                }
                if (msgBody.get("filename") != null) {
                    msg.put("filename", msgBody.get("filename").asText());
                }
                if (msgBody.get("file_length") != null) {
                    msg.put("msg_size", msgBody.get("file_length").asLong());
                }
                if (msgBody.get("secret") != null) {
                    msg.put("attachment_secret", msgBody.get("secret").asText());
                }
                if (msgBody.get("url") != null) {
                    msg.put("fileurl", msgBody.get("url").asText());
                }
                if (msgBody.get("size") != null && msgBody.get("size").get("width") != null && msgBody.get("size").get("height") != null) {
                    msg.put("pic_size", msgBody.get("size").get("width").asText() + "x" + msgBody.get("size").get("height").asText());
                }
                if (msgBody.get("length") != null) {
                    msg.put("msg_duration", msgBody.get("length").asInt());
                }
                if (msgBody.get("thumb") != null) {
                    msg.put("thumb_url", msgBody.get("thumb").asText());
                }
                if (msgBody.get("thumb_secret") != null) {
                    msg.put("thumb_secret", msgBody.get("thumb_secret").asText());
                }
                if (msgBody.get("length") != null) {
                    msg.put("msg_duration", msgBody.get("length").asInt());
                }
                if (msgBody.get("lat") != null && msgBody.get("lng") != null) {
                    msg.put("latitude", msgBody.get("lat").asDouble());
                    msg.put("longitude", msgBody.get("lng").asDouble());
                }
                if (msgBody.get("addr") != null) {
                    msg.put("address", msgBody.get("addr").asText());
                }
            } else if (msgType.equals("cmd") || msgType.equals("text")) {
                return null;
//                throw new Exception("This message should be discarded!");
            } else {
                try {
                    throw new Exception("Please support MsgType: " + msgType);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        } else {
            return null;
//            msg.put("payload", source.get("payload"));
//            throw new Exception("Message should contain payload record info");
        }
        return msg;
    }

}
