package producer

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

class Producer(private val topic: String) {
    private val bootstrapServer: String = "localhost:9092"
    private val logger: Logger = LoggerFactory.getLogger("Producer")
    private val prop: Properties = Properties()
    private var producer: KafkaProducer<String, Any> ? = null

    fun produce(value: Any) = run {
        val record: ProducerRecord<String, Any> = ProducerRecord<String, Any>(topic, value)
        producer?.send(record, Callback(
            fun(recordMetadata: RecordMetadata, e:Exception?) {
                if(e == null) {
                    logger.info("\n Key: " + record.key() + "\n")
                    logger.info("Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()
                    )
                } else {
                    logger.info("\n Failed to produce record \n")
                }
            }
        ) )
        producer?.flush()
    }

    init {
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
        prop.setProperty(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer"
        )
        prop.setProperty(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer"
        )
        producer = KafkaProducer<String, Any>(prop)
    }
}