package im.utils;



import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
//import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class GenericAvroSerializer implements Serializer<GenericRecord> {

    KafkaAvroSerializer inner;

    public GenericAvroSerializer() {
        inner = new KafkaAvroSerializer();
    }

    public GenericAvroSerializer(SchemaRegistryClient client) {
        inner = new KafkaAvroSerializer(client);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);

    }

    @Override
    public byte[] serialize(String topic, GenericRecord data) {
        return inner.serialize(topic, data);
    }

    @Override
    public void close() {
        inner.close();
    }
}