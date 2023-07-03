package consumer

/* Classe que implementa os consumidores de eventos kafka, recebe como parametros o topico que ira consumir e seu
 * group id */

import com.mongodb.ConnectionString
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.bson.Document
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
class SimpleConsumer(private val topic: String, private val groupId: String) {
    private val bootstrapServer: String = "localhost:39092"
    private val prop: Properties = Properties()

    private var connectionString = ConnectionString("mongodb://localhost:27017")
    private var mongoClient = MongoClients.create(connectionString)
    private var database = mongoClient.getDatabase("kafka-events")
    private var collection: MongoCollection<Document> = database.getCollection("events")

    init {
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  "org.apache.kafka.common.serialization.StringDeserializer")
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        val logger: Logger = LoggerFactory.getLogger("Consumer")
        val consumer: KafkaConsumer<String, String> = KafkaConsumer<String, String>(prop)

        consumer.subscribe(listOf(topic))

        while (true) {
            val records : ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(200))
            for (record in records ) {
                val document = Document("key", record.key())
                    .append("value", record.value())
                collection.insertOne(document)

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


