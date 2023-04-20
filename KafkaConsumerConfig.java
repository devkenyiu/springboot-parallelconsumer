@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group.id}")
    private String groupId;

    @Value("${kafka.consumer.thread.count}")
    private int threadCount;

    @Bean
    public ParallelConsumer<String, String> kafkaConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ParallelConsumer<String, String> consumer = ParallelConsumer.create(properties);
        consumer.setRuntimeExecutor(Executors.newFixedThreadPool(threadCount));
        return consumer;
    }
}
