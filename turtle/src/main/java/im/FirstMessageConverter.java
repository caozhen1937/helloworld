package im;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.io.File;
import java.util.Collections;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import im.utils.GenericAvroSerde;
/**
 * Created by dell on 2017/5/5.
 */
public class FirstMessageConverter {

//     public static void main (String[] args)throws Exception{
//          FirstMessageConverter firstMessageConverter = new FirstMessageConverter();
//          firstMessageConverter.run();
//
//     }

     Properties properties;

     public FirstMessageConverter(Properties properties) {
          this.properties = properties;
     }
     public void run()throws Exception{
          Properties streamsConfiguration = new Properties();

          //"cdp-bj1-kafka-sr1:9092"
          String bootstrapServers = this.properties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
          //"http://cdp-bj1-kafka-sr2:8081"
          String schemaRegistryUrl = "http://" + this.properties.getProperty("schema.registry.url", "localhost:8081");

          String inputTopic = properties.getProperty("inputTopic","StartDataTopic");

          streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "toAvro");
          streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
          streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
          streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
          streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
          streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
          streamsConfiguration.put("schema.registry.url", schemaRegistryUrl);

          String outTopic = properties.getProperty("outputTopic", "ToAvroTopic");

          final Schema schema = new Schema.Parser().parse(new File("/root/user.avro"));
          final Serde<String> stringSerde = Serdes.String();
          final Serde<Long> longSerde = Serdes.Long();

          boolean isKeySerde = false;
          GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
          genericAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
                  isKeySerde);


          KStreamBuilder builder = new KStreamBuilder();
          KStream<String, String> inStream = builder.stream(stringSerde, stringSerde, inputTopic);

          KStream<String, GenericRecord> outStream = inStream.mapValues(value -> {
               GenericRecord msg = this.convertToCdpAvro(value, schema);
               return msg;
          }).filter((key, value) -> value != null);
          outStream.to(stringSerde, genericAvroSerde, outTopic);


          KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

          streams.start();

          // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
          Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
     }

     public  GenericRecord convertToCdpAvro(String recordStr, Schema schema) {

          Map<String, String> allInfo = new HashMap<String, String>();

          String substring = recordStr.substring(92);
          String[] split3 = substring.split(", ");


          for (int i = 0; i < split3.length; i++) {

               if (split3[i].indexOf("=") > -1) {

                    if (split3[i].indexOf("})") > -1) {
                         String[] split4 = split3[i].split("}")[0].split("=");
                         allInfo.put(split4[0], split4[1]);
                    }else {
                         String[] split5 = split3[i].split("=");
                         allInfo.put(split5[0], split5[1]);
                    }

               }
          }
          GenericRecord msg = new GenericData.Record(schema);

          msg.put("created", Long.valueOf(allInfo.get("created")));
          msg.put("modified", Long.valueOf(allInfo.get("modified")));
          msg.put("model", allInfo.get("model"));
          msg.put("type", allInfo.get("type"));
          msg.put("uuid", allInfo.get("uuid"));

          if (allInfo.get("name")!=null) {
               msg.put("name", allInfo.get("name"));
          }
          if (allInfo.get("token")!=null) {
               msg.put("token", allInfo.get("token"));
          }
          if (allInfo.get("app")!=null) {
               msg.put("app", allInfo.get("app"));
          }
          if (allInfo.get("appkey")!=null) {
               msg.put("appkey", allInfo.get("appkey"));
          }
          if (allInfo.get("easemob.version")!=null) {
               msg.put("easemob_version", allInfo.get("easemob.version"));
          }
          if (allInfo.get("imei")!=null) {
               msg.put("imei", allInfo.get("imei"));
          }
          if (allInfo.get("loc.lat")!=null) {
               msg.put("loc_lat", Double.valueOf(allInfo.get("loc.lat")));
          }
          if (allInfo.get("loc.lng")!=null) {
               msg.put("loc_lng", Double.valueOf(allInfo.get("loc.lng")));
          }
          if (allInfo.get("binded_user")!=null) {
               msg.put("binded_user", allInfo.get("binded_user"));
          }
          if (allInfo.get("os_version")!=null) {
               msg.put("os_version", allInfo.get("os_version"));
          }
          if (allInfo.get("token_inactive")!=null) {
               msg.put("token_inactive", Boolean.valueOf(allInfo.get("token_inactive")));
          }
          if (allInfo.get("userid")!=null) {
               msg.put("userid", allInfo.get("userid"));
          }
          if (allInfo.get("manufacturer")!=null) {
               msg.put("manufacturer", allInfo.get("manufacturer"));
          }
          if (allInfo.get("operator")!=null) {
               msg.put("operator", allInfo.get("operator"));
          }
          if (allInfo.get("version")!=null) {
               msg.put("version", allInfo.get("version"));
          }
          if (allInfo.get("deviceid")!=null) {
               msg.put("deviceid", Integer.valueOf(allInfo.get("deviceid")));
          }

          return msg;
     }

}
