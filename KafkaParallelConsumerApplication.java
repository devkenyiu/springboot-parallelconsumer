import com.google.gson.Gson;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;
import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.offsets.OffsetMapCodec;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class KafkaParallelConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaParallelConsumerApplication.class, args);
    }

    @Bean
    public ParallelConsumer<String, GenericRecord> parallelConsumer(@Value("${kafka.bootstrap.servers}") String bootstrapServers,
                                                                      @Value("${kafka.schema.registry.url}") String schemaRegistryUrl,
                                                                      @Value("${kafka.topic.name}") String topicName,
                                                                      @Value("${kafka.group.id}") String groupId,
                                                                      @Value("${kafka.consumer.threads}") int numThreads) {

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerConfig.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        consumerConfig.put(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        consumerConfig.put(OffsetMapCodec.class.getName(), new OffsetMapCodec());
        DefaultKafkaConsumerFactory<String, GenericRecord> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfig);

        ParallelConsumerOptions<String, GenericRecord> options = ParallelConsumerOptions.<String, GenericRecord>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .disableAutomaticCommit()
                .build();

        ParallelConsumer<String, GenericRecord> parallelConsumer = ParallelConsumer.create(consumerFactory, options);
        parallelConsumer.subscribe(topicName);
        parallelConsumer.setRuntimeExecutor(numThreads);
        parallelConsumer.setCheckpointPersistFrequencyMs(TimeUnit.MINUTES.toMillis(1));
        parallelConsumer.setCommitMaxBatchSize(1000);
        parallelConsumer.setCommitMaxBatchTimeMs(500);
        parallelConsumer.setCommitMaxRetries(10);
       
    return parallelConsumer;
}

@KafkaListener(id = "parallelConsumer", topics = "${kafka.topic.name}")
public void processPersonMessage(GenericRecord message) {
    // Process the message
    System.out.println("Received message: " + message);
}

@Bean
public ConcurrentKafkaListenerContainerFactory<String, GenericRecord> kafkaListenerContainerFactory(
        @Value("${kafka.bootstrap.servers}") String bootstrapServers,
        @Value("${kafka.schema.registry.url}") String schemaRegistryUrl,
        @Value("${kafka.group.id}") String groupId) {

    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerConfigs(bootstrapServers, schemaRegistryUrl, groupId)));
    factory.setConcurrency(3);
    factory.setAutoStartup(false);
    return factory;
}

private Map<String, Object> consumerConfigs(String bootstrapServers, String schemaRegistryUrl, String groupId) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class);
    props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    props.put(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
    return props;
}

@Bean
public KafkaTemplate<String, Object> kafkaTemplate(@Value("${kafka.bootstrap.servers}") String bootstrapServers,
                                                   @Value("${kafka.schema.registry.url}") String schemaRegistryUrl) {

    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerConfig.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerConfig,
            new StringSerializer(), new KafkaAvroSerializer()));
    return kafkaTemplate;
}
}
