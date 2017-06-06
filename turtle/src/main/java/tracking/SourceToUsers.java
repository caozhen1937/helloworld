package tracking;

/**
 * Created by yuchaoma on 17/04/2017.
 */



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
import java.text.ParseException;
import java.util.*;

import static tracking.Utils.fillAvroFromJson;
import static tracking.Utils.toSeconds;

public class SourceToUsers {

    Properties properties;
    public SourceToUsers(Properties properties){
        this.properties = properties;
    }

    //TODO Log Errors
    public void run() throws IOException {
        // multiple source , multiple bootstrap servers
        Properties streamsConfiguration = new Properties();
        String bootstrapServers = this.properties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        String schemaRegistryUrl = "http://" +  this.properties.getProperty("schema.registry.url", "localhost:8081");
        String applicationId = "Tracking Source To Users";
        String inputTopic = properties.getProperty("inputTopic", "src-tracking");
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put("schema.registry.url", schemaRegistryUrl);
        String outTopic =properties.getProperty("outputTopic", "dev_tracking_users");
        final Schema schema = new Schema.Parser().parse(new File("src/main/java/tracking/users.avro"));

        boolean isKeySerde = false;
        GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
        genericAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
                isKeySerde);


        final Serde<String> stringSerde = Serdes.String();

        KStreamBuilder builder = new KStreamBuilder();


        KStream<String, String> inStream = builder.stream(stringSerde, stringSerde, inputTopic);

        KStream<String, GenericRecord> outStream =inStream.mapValues(value -> {
            GenericRecord msg = null;
            try {
                try {
                    msg = this.convertUsersToAvro(value, schema);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
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


    public GenericRecord convertUsersToAvro(String value, Schema schema) throws IOException, ParseException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode source = null;
        source = mapper.readTree(value);
        String inputType = source.get("type").asText();
        switch (inputType){
            case "identify":
                return convertIdentifyToAvro(source, schema);
            case "alias":
                return convertAliasToAvro(source, schema);
            default:
                return null;
        }
    }




    public GenericRecord convertIdentifyToAvro(JsonNode source, Schema schema) throws IOException, ParseException {

        GenericRecord user = new GenericData.Record(schema);
        user.put("appKey", source.get("writeKey").asText());
        user.put("anonymousId", source.get("anonymousId").asText());
        user.put("ts", toSeconds(source.get("timestamp").asText()));
        HashMap<String, Object[]> fieldMappings = commonFieldMappings();
        fieldMappings.put("username", new Object[] {"String", new String[]{"traits", "name"}} );
        fieldMappings.put("email", new Object[] {"String", new String[]{"traits", "email"}} );
        fieldMappings.put("plan", new Object[] {"String", new String[]{"traits", "plan"}} );
        fieldMappings.put("street", new Object[] {"String", new String[]{"traits","address", "street"}} );
        fieldMappings.put("city", new Object[] {"String", new String[]{"traits","address", "city"}} );
        fieldMappings.put("postalCode", new Object[] {"String", new String[]{"traits","address", "postalCode"}} );
        fieldMappings.put("country", new Object[] {"String", new String[]{"traits","address", "country"}} );

        fieldMappings.put("userAgent", new Object[] {"String", new String[]{"context", "userAgent"}} );
        fieldMappings.put("locale", new Object[] {"String", new String[]{"context", "locale"}} );
        fieldMappings.put("appVersion", new Object[] {"String", new String[]{"context", "app", "version"}} );
        fieldMappings.put("appNamespace", new Object[] {"String", new String[]{"context", "app", "namespace"}} );
        fieldMappings.put("appBuild", new Object[] {"String", new String[]{"context", "app", "build"}} );
        fieldMappings.put("appName", new Object[] {"String", new String[]{"context", "app", "name"}} );
        fieldMappings.put("deviceModel", new Object[] {"String", new String[]{"context", "device", "model"}} );
        fieldMappings.put("deviceId", new Object[] {"String", new String[]{"context", "device", "id"}} );
        fieldMappings.put("deviceName", new Object[] {"String", new String[]{"context", "device", "name"}} );
        fieldMappings.put("deviceManufacturer", new Object[] {"String", new String[]{"context", "device", "manufacturer"}} );
        fieldMappings.put("timezone", new Object[] {"String", new String[]{"context", "timezone"}} );
        fieldMappings.put("osVersion", new Object[] {"String", new String[]{"context", "os", "version"}} );
        fieldMappings.put("osName", new Object[] {"String", new String[]{"context", "os", "name"}} );
        fieldMappings.put("screenWidth", new Object[] {"Int", new String[]{"context", "screen", "width"}} );
        fieldMappings.put("screenHeight", new Object[] {"Int", new String[]{"context", "screen", "height"}} );
        fieldMappings.put("screenDensity", new Object[] {"Double", new String[]{"context", "screen", "density"}} );
        fieldMappings.put("channel", new Object[] {"String", new String[]{"channel"}} );


        fieldMappings.put("website", new Object[] {"String", new String[]{"traits","website"}} );
        fieldMappings.put("title", new Object[] {"String", new String[]{"traits","title"}} );
        fieldMappings.put("phone", new Object[] {"String", new String[]{"traits","phone"}} );
        fieldMappings.put("userDbId", new Object[] {"String", new String[]{"traits","id"}} );
        fieldMappings.put("gender", new Object[] {"String", new String[]{"traits","gender"}} );
        fieldMappings.put("name", new Object[] {"String", new String[]{"traits","name"}} );
        fieldMappings.put("description", new Object[] {"String", new String[]{"traits","description"}} );
        fieldMappings.put("birthday", new Object[] {"String", new String[]{"traits","birthday"}} );
        fieldMappings.put("avatar", new Object[] {"String", new String[]{"traits","avatar"}} );
        fieldMappings.put("age", new Object[] {"Int", new String[]{"traits","age"}} );

        fieldMappings.put("properties", new Object[] {"String", new String[]{"traits"}} );


        user = fillAvroFromJson(user, source, fieldMappings);
        return user;
    }



    //TODO how to deal with idenfiy logs?
    private HashMap<String, Object[]> commonFieldMappings(){
        HashMap<String, Object[]> fieldMappings = new HashMap<>();
        fieldMappings.put("channel", new Object[] {"String", new String[]{"channel"}} );
        fieldMappings.put("userAgent", new Object[] {"String", new String[]{"context", "userAgent"}} );
        fieldMappings.put("ip", new Object[] {"String", new String[]{"context", "ip"}} );
        // TODO how to deal with messageId / Version here?
        fieldMappings.put("userId", new Object[] {"String", new String[]{"userId"}} );
        fieldMappings.put("previousId", new Object[] {"String", new String[]{"previousId"}} );
        return fieldMappings;
    }

    public GenericRecord convertAliasToAvro(JsonNode source, Schema schema) throws IOException, ParseException {
        GenericRecord user = new GenericData.Record(schema);
        user.put("appKey", source.get("appKey").asText());
        user.put("anonymousId", source.get("anonymousId").asText());
        user.put("ts", toSeconds(source.get("timestamp").asText()));
        HashMap<String, Object[]> fieldMappings = commonFieldMappings();
        user = fillAvroFromJson(user, source, fieldMappings);
        return user;
    }





}
