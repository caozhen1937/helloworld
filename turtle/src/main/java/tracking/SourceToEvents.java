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
import java.security.Timestamp;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParseException;
import java.text.ParsePosition;
import java.util.*;

import static tracking.Utils.fillAvroFromJson;
import static tracking.Utils.toSeconds;

public class SourceToEvents {

    Properties properties;
    public SourceToEvents(Properties properties){
        this.properties = properties;
    }

    //TODO Log Errors
    /*
    * Invoke this method to consume data
    */
    public void run() throws IOException {
        // multiple source , multiple bootstrap servers
        Properties streamsConfiguration = new Properties();
        String bootstrapServers = this.properties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        String schemaRegistryUrl = "http://" +  this.properties.getProperty("schema.registry.url", "localhost:8081");
        String inputTopic = properties.getProperty("inputTopic", "src-tracking");
        String applicationId = "Tracking Source To Events";
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put("schema.registry.url", schemaRegistryUrl);
        String outTopic =properties.getProperty("outputTopic", "dev_tracking_events");
        final Schema schema = new Schema.Parser().parse(new File("src/main/java/tracking/events.avro"));

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
                msg = this.convertToEventsAvro(value, schema);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
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



    public GenericRecord convertToEventsAvro(String value, Schema schema) throws IOException, ParseException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode source = null;
        source = mapper.readTree(value);
        String inputType = source.get("type").asText();
        if(  !Arrays.asList(new String[]{"track", "screen", "page"}).contains(inputType)){
            return null;
        }
        return convertTrackToEventsAvro(source, schema);


    }


    public GenericRecord convertTrackToEventsAvro(JsonNode source, Schema schema) throws IOException, ParseException {
        GenericRecord event = new GenericData.Record(schema);
        event.put("appKey", source.get("writeKey").asText());
        event.put("anonymousId", source.get("anonymousId").asText());

        String inputType = source.get("type").asText();
        switch (inputType){
            case "track":
                event.put("event", source.get("event").asText());
                break;
            case "screen":
                event.put("event", "__SCREEN__");
                break;
            case "page":
                event.put("event", "__PAGE__");
                break;
            default:
                return null;
        }
        event.put("ts", toSeconds(source.get("timestamp").asText()));
        event.put("eventId", source.get("messageId").asText());

        if(source.get("sentAt") != null){
            event.put("sentAt", toSeconds(source.get("sentAt").asText()));
        }
        if(source.get("receivedAt") != null) {
            event.put("receivedAt", toSeconds(source.get("receivedAt").asText()));
        }
        if(source.get("originalTimestamp") != null) {
            event.put("originalTimestamp", toSeconds(source.get("originalTimestamp").asText()));
        }

        HashMap<String, Object[]> fieldMappings = commonFieldMappings();
        event = fillAvroFromJson(event, source, fieldMappings);
        return event;
    }




    private HashMap<String, Object[]> commonFieldMappings(){
        HashMap<String, Object[]> fieldMappings = new HashMap<>();
        fieldMappings.put("libraryName", new Object[] {"String", new String[]{"context", "library", "name"}} );
        fieldMappings.put("libraryVersion", new Object[] {"String", new String[]{"context", "library", "version"}} );
        fieldMappings.put("pagePath", new Object[] {"String", new String[]{"context", "page", "path"}} );
        fieldMappings.put("pageRefer", new Object[] {"String", new String[]{"context", "page", "referrer"}} );
        fieldMappings.put("pageSearch", new Object[] {"String", new String[]{"context", "page", "search"}} );
        fieldMappings.put("pageTitle", new Object[] {"String", new String[]{"context", "page", "title"}} );
        fieldMappings.put("pageURL", new Object[] {"String", new String[]{"context", "page", "url"}} );
        fieldMappings.put("userAgent", new Object[] {"String", new String[]{"context", "userAgent"}} );

        fieldMappings.put("networkWifi", new Object[] {"Boolean", new String[]{"context", "network", "wifi"}} );
        fieldMappings.put("networkBluetooth", new Object[] {"Boolean", new String[]{"context", "network", "bluetooth"}} );
        fieldMappings.put("networkCellular", new Object[] {"Boolean", new String[]{"context", "network", "cellular"}} );
        fieldMappings.put("networkCarrier", new Object[] {"String", new String[]{"context", "network", "carrier"}} );
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



        fieldMappings.put("ip", new Object[] {"String", new String[]{"context", "ip"}} );
        fieldMappings.put("userId", new Object[] {"String", new String[]{"userId"}} );
        fieldMappings.put("properties", new Object[] {"String", new String[]{"properties"}} );
        return fieldMappings;
    }




}
