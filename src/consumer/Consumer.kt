package consumer

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties;

class Consumer(private val topic: String, private val groupId: String) {
    private val bootstrapServer: String = "localhost:9092"
    private val prop: Properties = Properties()

    init {
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        val logger: Logger = LoggerFactory.getLogger("Consumer");
        val consumer: KafkaConsumer<String, Any> = KafkaConsumer<String, Any>(prop)

        consumer.subscribe(listOf(topic))
        while (true) {
            val records : ConsumerRecords<String, Any> = consumer.poll(Duration.ofMillis(200))

            for (record in records ) {
                logger.info("Key: " + record.key() + "Value " + record.value())
            }
        }
    }
}

