package tracking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import kafka.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.*;

import static kafka.consumer.Consumer.createJavaConsumerConnector;
import static tracking.Utils.fillRowFromAvro;

/**
 * Created by yuchaoma on 17/04/2017.
 */
public class WriteEventsToKudu  {


    private KafkaStream stream;
    private KuduClient dbClient;


    public WriteEventsToKudu(Properties argsProp) {

        String zookeeper = argsProp.getProperty("zookeeper", "localhost:2181");
        String schemaRegistryUrl = "http://" +  argsProp.getProperty("schema.registry.url", "localhost:8081");

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
        KuduTable eventsTable = null;
        KuduTable rawEventsTable = null;
        KuduTable usersTable = null;
        KuduTable userActivitiesTable = null;
        KuduTable propertyNamesTable = null;

        try {
            eventsTable = this.dbClient.openTable("impala::tracking_dev.events");
            rawEventsTable = this.dbClient.openTable("impala::tracking_dev.raw_events");
            usersTable = this.dbClient.openTable("impala::tracking_dev.users");
            userActivitiesTable = this.dbClient.openTable("impala::tracking_dev.user_activities");
            propertyNamesTable = this.dbClient.openTable("impala::tracking_dev.property_names");
        } catch (KuduException e) {
            e.printStackTrace();
        }



        KuduSession session = dbClient.newSession();
        ConsumerIterator it = this.stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata messageAndMetadata = it.next();
//            String key = (String) messageAndMetadata.key();
            GenericRecord eventRecord = (GenericRecord) messageAndMetadata.message();
            Insert eventsInsert = insertToEvents(eventRecord, eventsTable);
            Insert rawEventsInsert = insertToEvents(eventRecord, rawEventsTable);
            RowResult userRow = Utils.getUser(eventRecord, usersTable, dbClient);
            Update usersUpdate = null;
            Insert usersInsert = null;

            if(userRow != null){
                usersUpdate = updateUsers(eventRecord, usersTable, userRow);
            }else {
                usersInsert = insertToUsers(eventRecord, usersTable);
            }
            Insert userActivitiesInsert = insertToUserActivities(eventRecord, userActivitiesTable);
            try {
                session.apply(eventsInsert);
                session.apply(rawEventsInsert);
                if(usersUpdate != null){
                    session.apply(usersUpdate);
                }
                if(usersInsert != null){
                    session.apply(usersInsert);
                }
                if( eventRecord.get("properties") != null){
                    String appKey = eventRecord.get("appKey").toString();
                    String event = eventRecord.get("event").toString();
                    String propertiesStr = eventRecord.get("properties").toString();
                    Utils.insertToEventPropertyNames(appKey, event , propertiesStr, propertyNamesTable, session);
                }
                session.apply(userActivitiesInsert);
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }






    private Insert insertToEvents(GenericRecord eventRecord, KuduTable table){
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();

        row.addString("appkey", eventRecord.get("appKey").toString());
        row.addString("event", eventRecord.get("event").toString());
        row.addLong("ts", (long) eventRecord.get("ts"));
        row.addString("anonymousid", eventRecord.get("anonymousId").toString());
        row.addString("eventid", eventRecord.get("eventId").toString());
//        row.addString("userid", eventRecord.get("userId").toString());
//        row.addString("properties", eventRecord.get("properties").toString());

        HashMap<String, String[]> fieldMapping = new HashMap<>();
        fieldMapping.put("libraryname", new String[]{"String", "libraryName"});
        fieldMapping.put("libraryversion", new String[]{"String", "libraryVersion"});
        fieldMapping.put("pagepath", new String[]{"String", "pagePath"});
        fieldMapping.put("pagerefer", new String[]{"String", "pageRefer"});
        fieldMapping.put("pagesearch", new String[]{"String", "pageSearch"});
        fieldMapping.put("pagetitle", new String[]{"String", "pageTitle"});
        fieldMapping.put("pageurl", new String[]{"String", "pageURL"});
        fieldMapping.put("useragent", new String[]{"String", "userAgent"});
        fieldMapping.put("pagename", new String[]{"String", "pageName"});
        fieldMapping.put("screenname", new String[]{"String", "screenName"});
        fieldMapping.put("ip", new String[]{"String", "ip"});
        fieldMapping.put("receivedat", new String[]{"Long", "receivedAt"});
        fieldMapping.put("sentat", new String[]{"Long", "sentAt"});
        fieldMapping.put("revenue", new String[]{"Double", "revenue"});
        fieldMapping.put("intrinsicvalue", new String[]{"Double", "intrinsicValue"});
        fieldMapping.put("properties", new String[]{"String", "properties"});
        fieldMapping.put("userid", new String[]{"String", "userId"});


        fieldMapping.put("networkwifi", new String[]{"Boolean", "networkWifi"});
        fieldMapping.put("networkbluetooth", new String[]{"Boolean", "networkBluetooth"});
        fieldMapping.put("networkcellular", new String[]{"Boolean", "networkCellular"});
        fieldMapping.put("networkcarrier", new String[]{"String", "networkCarrier"});
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


        fillRowFromAvro(row, eventRecord, fieldMapping);


        return insert;
    }



    private Insert insertToUsers(GenericRecord eventRecord, KuduTable table){
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addString("appkey", eventRecord.get("appKey").toString());
        row.addString("anonymousid", eventRecord.get("anonymousId").toString());
        if(eventRecord.get("userId") != null){
            row.addString("userid", eventRecord.get("userId").toString());
        }
        row.addLong("lastseen", (long) eventRecord.get("ts"));
        if(eventRecord.get("revenue") != null){
            row.addDouble("totalrevenue", (double) eventRecord.get("revenue") );
        }
        if(eventRecord.get("intrinsicValue") != null){
            row.addDouble("totalvalue", (double) eventRecord.get("intrinsicValue"));
        }
        return insert;

    }

    private Update updateUsers(GenericRecord eventRecord, KuduTable table, RowResult userRow){
        Update update = table.newUpdate();
        PartialRow row = update.getRow();
        row.addString("appkey", eventRecord.get("appKey").toString());
        row.addString("anonymousid", eventRecord.get("anonymousId").toString());
        if(eventRecord.get("userId") != null){
            row.addString("userid", eventRecord.get("userId").toString());
        }
        row.addLong("lastseen", (long) eventRecord.get("ts"));
        if(eventRecord.get("revenue") != null){
            row.addDouble("totalrevenue", (double) eventRecord.get("revenue") + userRow.getDouble("totalrevenue"));
        }
        if(eventRecord.get("intrinsicValue") != null){
            row.addDouble("totalvalue", (double) eventRecord.get("intrinsicValue") + userRow.getDouble("totalvalue"));
        }
        return update;
    }

    private Insert insertToUserActivities(GenericRecord eventRecord, KuduTable table){
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addString("appkey", eventRecord.get("appKey").toString());
        row.addString("event", eventRecord.get("event").toString());
        row.addLong("ts", (long) eventRecord.get("ts"));
        row.addString("anonymousid", eventRecord.get("anonymousId").toString());
        row.addString("eventid", eventRecord.get("eventId").toString());
        if(eventRecord.get("userId") != null) {
            row.addString("userid", eventRecord.get("userId").toString());
        }

        return insert;
    }


}
