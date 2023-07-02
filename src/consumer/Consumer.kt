package consumer

/* Classe que implementa os consumidores de eventos kafka, recebe como parametros o topico que ira consumir e seu
 * group id */

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.TimeWindows
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
class Consumer(private val topic: String, private val groupId: String) {
    private val bootstrapServer: String = "localhost:39092"
    private val prop: Properties = Properties()
    private val streamConfig: Properties = Properties()
    private val propProducer: Properties = Properties()

    init {
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  "org.apache.kafka.common.serialization.StringDeserializer")
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


        streamConfig[StreamsConfig.APPLICATION_ID_CONFIG] = "event-count-app"
        streamConfig[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        streamConfig[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:39092"
        streamConfig[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name

        propProducer.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
        propProducer.setProperty(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer"
        )
        propProducer.setProperty(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer"
        )

        val logger: Logger = LoggerFactory.getLogger("Consumer")
        val consumer: KafkaConsumer<String, String> = KafkaConsumer<String, String>(prop)

        consumer.subscribe(listOf(topic))

        val builder = StreamsBuilder()
        val inputTopic = topic
        val outputTopic = "DANGEROUS_ROAD"

        val events: KStream<String, String> = builder.stream(inputTopic)

        var info: String? = null

        val eventCounts = events
            .peek { key, value ->
                println("Chave1: $key, Valor1: $value")
                info = value
            }
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofMinutes(1))) // Janela temporal de 5 minutos
            .count()
            .toStream()
            .filter { _, count -> count == 1L }
            .map { key, _ -> KeyValue(key.key(), info) }

        eventCounts.to(outputTopic)

        val streams = KafkaStreams(builder.build(), streamConfig)
        streams.start()

        while (true) {
            val records : ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(200))
            for (record in records ) {
                logger.info(
                            "\n" + "\n" + "\n" +
                            "Key: " + record.key() + "\n" +
                            "Value: " + record.value() + "\n" +
                            "\n" + "\n" + "\n")
            }
            Thread.sleep(1000)
        }
    }
}


