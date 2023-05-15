package producer

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

/* Classe que implementa os produtores de eventos kafka primitivos, recebe como parametro o topico para o qual ira produzir */

class Producer(private val topic: String) {
    private val bootstrapServer: String = "localhost:29092"
    private val logger: Logger = LoggerFactory.getLogger("Producer")
    private val prop: Properties = Properties()
    private var producer: KafkaProducer<String, Any> ? = null

    /*  Função que produz um evento primitivo, enviando a mensagem 'value' passada como parametro contendo
        o tipo de alerta, a rua e o timestamp do alerta que gerou o evento, executa um callback contendo informacoes de sucesso ou erro
    */
    fun produce(value: Any, key: String) = run {
        val record: ProducerRecord<String, Any> = ProducerRecord<String, Any>(topic, key, value)
        producer?.send(record, Callback(
            fun(recordMetadata: RecordMetadata, e:Exception?) {
                if(e == null) {
                    logger.info(
                                "\n" + "\n" + "\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Key: " + record.key() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() +
                                "\n" + "\n" + "\n"
                    )
                } else {
                    logger.info("\n Failed to produce record \n")
                }
            }
        ) )
        producer?.flush()
    }

    /*  Função que eh executada ao iniciar o objeto, configura as propriedades do produtor */
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