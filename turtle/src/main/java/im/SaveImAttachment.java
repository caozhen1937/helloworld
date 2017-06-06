package im;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import kafka.consumer.ConsumerConfig;

import java.io.*;
import java.math.BigInteger;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static kafka.consumer.Consumer.createJavaConsumerConnector;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by yuchaoma on 20/04/2017.
 */
public class SaveImAttachment {



        private KafkaStream stream;
        private HTable htable;


        public SaveImAttachment(Properties argsProp) throws IOException {

            String zookeeper = argsProp.getProperty("zookeeper", "localhost:2181");
            String schemaRegistryUrl = "http://" +  argsProp.getProperty("schema.registry.url", "localhost:8081");
            String dbName = argsProp.getProperty("htable.name", "im_attach_dev");
            String groupId = argsProp.getProperty("group.id", "dev-im-attachment");
            String topic = argsProp.getProperty("inputTopic");
            Properties props = new Properties();
            props.put("zookeeper.connect", zookeeper);
            props.put("group.id", groupId);
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

            Configuration config = HBaseConfiguration.create();

            config.set("hbase.zookeeper.property.clientPort", zookeeper.split(":")[1]);
            config.set("hbase.zookeeper.quorum", zookeeper.split(":")[0]);
            config.set("zookeeper.znode.parent", "/hbase");
            this.htable = new HTable(config, dbName);

        }







        public void run() throws IOException {

            ConsumerIterator it = this.stream.iterator();
            String[] validTypes =  new String[]{"file", "video", "audio", "img"};
            String[] columns = new String[]{"fileurl", "attachment_secret", "msg_size", "filename", "msg_type",
                    "msg_duration", "thumb_secret", "thumb_url", "pic_size", "send_time" };

            while (it.hasNext()) {
                MessageAndMetadata messageAndMetadata = it.next();
                GenericRecord messageRecord = (GenericRecord) messageAndMetadata.message();
                if(Arrays.asList(validTypes).contains(messageRecord.get("msg_type").toString())){
                    if(messageRecord.get("fileurl") == null){
                        continue;
                    }
//                    boolean isOffline = (messageRecord.get("receiver_absent") != null && (boolean)messageRecord.get("receiver_absent"));
//                    if(messageRecord.get("receive_time") != null ||  isOffline){
//                        continue;
//                    }
                    String fileURL = messageRecord.get("fileurl").toString();
                    MessageDigest md = null;
                    try {
                        md = MessageDigest.getInstance("MD5");
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                        continue;
                    }
                    md.update(fileURL.getBytes());
                    String key = new BigInteger(1, md.digest()).toString(16);
                    Put p = new Put(Bytes.toBytes(key));
                    for(String column: columns){
                        if(messageRecord.get(column) != null){
                            byte[]  value = Bytes.toBytes(messageRecord.get(column).toString());
                            p.addColumn(Bytes.toBytes("f"), Bytes.toBytes(column), value);
                        }
                    }
                    byte[] fileBytes = getFileBytes(messageRecord.get("fileurl").toString());
                    p.addColumn(Bytes.toBytes("f"), Bytes.toBytes("bytes"), fileBytes);
                    if(messageRecord.get("thumb_url") != null){
                        byte[] thumbBytes = getFileBytes(messageRecord.get("thumb_url").toString());
                        p.addColumn(Bytes.toBytes("f"), Bytes.toBytes("thumb_bytes"), thumbBytes);
                    }
                    this.htable.put(p);
                }

            }
        }


        private byte[] getFileBytes(String url) throws IOException {
            URL source = new URL(url);
            InputStream input = source.openStream();
            ByteArrayOutputStream output;
            try {
                output = new ByteArrayOutputStream();
                try {
                    IOUtils.copy(input, output);
                } finally {
                    IOUtils.closeQuietly(output);
                }
            } finally {
                IOUtils.closeQuietly(input);
            }
            return Bytes.toBytes(output.toString());
        }
}
