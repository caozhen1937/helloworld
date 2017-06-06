package tracking;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kudu.client.*;
import kafka.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.*;

import static kafka.consumer.Consumer.createJavaConsumerConnector;
import static tracking.Utils.fillRowFromAvro;

/**
 * Created by yuchaoma on 17/04/2017.
 */
public class WriteUsersToKudu  {


    private KafkaStream stream;
    private KuduClient dbClient;


    public WriteUsersToKudu(Properties argsProp) {

        String zookeeper = argsProp.getProperty("zookeeper", "localhost:2181");
        String schemaRegistryUrl = "http://" +  argsProp.getProperty("schema.registry.url", "localhost:8081");
        String kuduMaster =  argsProp.getProperty("kudu.master", "cdp-bj1-hdfs-server3:7051");

        String topic = argsProp.getProperty("inputTopic");
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", "kudu-consumer");
        props.put("schema.registry.url", schemaRegistryUrl);

        VerifiableProperties vProps = new VerifiableProperties(props);

        KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
        KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);

        ConsumerConnector consumer;
        consumer = createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<Object, Object>>> consumerMap = consumer.createMessageStreams(
                topicCountMap, keyDecoder, valueDecoder);
        this.stream = consumerMap.get(topic).get(0);
        this.dbClient = new KuduClient.KuduClientBuilder("cdp-bj1-hdfs-server3:7051").build();

    }



    public void run() throws IOException {
        KuduTable usersTable = null;
        KuduTable propertyNamesTable = null;
        try {
            usersTable = this.dbClient.openTable("impala::tracking_dev.users");
            propertyNamesTable = this.dbClient.openTable("impala::tracking_dev.property_names");
        } catch (KuduException e) {
            e.printStackTrace();
        }

        KuduSession session = dbClient.newSession();
        ConsumerIterator it = this.stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata messageAndMetadata = it.next();
//            String key = (String) messageAndMetadata.key();
            GenericRecord userRecord = (GenericRecord) messageAndMetadata.message();

            RowResult userRow = Utils.getUser(userRecord, usersTable, dbClient);
            Insert usersInsert = null;
            Update usersUpdate = null;
            if(userRow == null){
                usersInsert =  insertToUsers(userRecord, usersTable);
            }else {
                usersUpdate = updateUsers(userRecord, usersTable, userRow);
            }
            try {
                if(usersInsert != null){
                    session.apply(usersInsert);
                } else{
                    session.apply(usersUpdate);
                }
                if( userRecord.get("properties") != null){
                    String appKey = userRecord.get("appKey").toString();
                    String event = "__USER__";
                    String propertiesStr = userRecord.get("properties").toString();
                    Utils.insertToEventPropertyNames(appKey, event , propertiesStr, propertyNamesTable, session);
                }
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }


    }


    private Insert insertToUsers(GenericRecord userRecord, KuduTable table){
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addString("appkey", userRecord.get("appKey").toString());
        row.addString("anonymousid", userRecord.get("anonymousId").toString());
        if(userRecord.get("userId") != null){
            row.addString("userid", userRecord.get("userId").toString());
        }
        row.addLong("lastseen", (long) userRecord.get("ts"));



        fillRowFromAvro(row, userRecord, commonFieldMappings());


        if(userRecord.get("properties") != null){
            row.addString("properties", userRecord.get("properties").toString());
        }
        return insert;

    }

    private HashMap<String, String[]> commonFieldMappings(){
        HashMap<String, String[]> fieldMapping = new HashMap<>();
        fieldMapping.put("city", new String[]{"String", "city"});
        fieldMapping.put("country", new String[]{"String", "country"});
        fieldMapping.put("postalcode", new String[]{"String", "postalCode"});
        fieldMapping.put("age", new String[]{"Int", "age"});
        fieldMapping.put("avatar", new String[]{"String", "avatar"});
        fieldMapping.put("birthday", new String[]{"String", "birthday"});
        fieldMapping.put("createat", new String[]{"Long", "createdAt"});
        fieldMapping.put("description", new String[]{"String", "description"});
        fieldMapping.put("email", new String[]{"String", "email"});
        fieldMapping.put("name", new String[]{"String", "name"});
        fieldMapping.put("gender", new String[]{"String", "gender"});
        fieldMapping.put("userdbid", new String[]{"String", "userDbId"});
        fieldMapping.put("phone", new String[]{"String", "phone"});
        fieldMapping.put("title", new String[]{"String", "title"});
        fieldMapping.put("username", new String[]{"String", "username"});
        fieldMapping.put("website", new String[]{"String", "website"});
        fieldMapping.put("browser", new String[]{"String", "browser"});
        fieldMapping.put("device", new String[]{"String", "device"});
        fieldMapping.put("os", new String[]{"String", "os"});

        fieldMapping.put("locale", new String[]{"String", "locale"});
        fieldMapping.put("appversion", new String[]{"String", "appVersion"});
        fieldMapping.put("appnamespace", new String[]{"String", "appNamespace"});
        fieldMapping.put("appbuild", new String[]{"String", "appBuild"});
        fieldMapping.put("appname", new String[]{"String", "appName"});
        fieldMapping.put("devicemodel", new String[]{"String", "deviceModel"});
        fieldMapping.put("deviceid", new String[]{"String", "deviceId"});
        fieldMapping.put("devicename", new String[]{"String", "deviceName"});
        fieldMapping.put("devicemanufacturer", new String[]{"String", "deviceManufacturer"});
        fieldMapping.put("timezone", new String[]{"String", "timezone"});
        fieldMapping.put("osversion", new String[]{"String", "osVersion"});
        fieldMapping.put("osname", new String[]{"String", "osName"});
        fieldMapping.put("screenwidth", new String[]{"Int", "screenWidth"});
        fieldMapping.put("screenheight", new String[]{"Int", "screenHeight"});
        fieldMapping.put("screendensity", new String[]{"Double", "screenDensity"});
        fieldMapping.put("channel", new String[]{"String", "channel"});

        return fieldMapping;
    }

    private Update updateUsers(GenericRecord userRecord, KuduTable table, RowResult userRow){
        Update update = table.newUpdate();
        PartialRow row = update.getRow();
        row.addString("appkey", userRecord.get("appKey").toString());
        row.addString("anonymousid", userRecord.get("anonymousId").toString());
        fillRowFromAvro(row, userRecord, commonFieldMappings());
        if(userRecord.get("userId") != null){
            row.addString("userid", userRecord.get("userId").toString());
        }
        row.addLong("lastseen", (long) userRecord.get("ts"));
        if(userRecord.get("properties") != null){
            row.addString("properties", userRecord.get("properties").toString());
        }
        return update;
    }

}
