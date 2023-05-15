package consumer

/* Classe que implementa os consumidores de eventos kafka, recebe como parametros o topico que ira consumir e seu
 * group id */

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import wazeApiMock.Alerts
import java.time.Duration
import java.time.LocalDateTime
import java.util.Properties
class Consumer(private val topic: String, private val groupId: String) {
    private val bootstrapServer: String = "localhost:29092"
    private val prop: Properties = Properties()
    private val propProducer: Properties = Properties()
    private val logger: Logger = LoggerFactory.getLogger("Producer")
    private var eventConsumed = mutableListOf<String>()

    private var producer: KafkaProducer<String, Any>? = null

    /*  Função que produz o evento complexo Dangerous Road, enviando a mensagem 'value' passada como parametro contendo
        o tipo de alerta e a rua que gerou o evento, executa um callback contendo informacoes de sucesso ou erro
    */
    private fun produce(value: Any) = run {
        val record: ProducerRecord<String, Any> = ProducerRecord<String, Any>("DANGEROUS_ROAD", value)
        producer?.send(record, Callback(
            fun(recordMetadata: RecordMetadata, e:Exception?) {
                if(e == null) {
                    logger.info(
                            "\n" + "\n" + "\n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Key: " + record.key() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp() + "\n" +
                            "Data: " + record.value() + "\n" +
                            "\n" + "\n" + "\n"
                    )
                } else {
                    logger.info("\n Failed to produce record \n")
                }
            }
        ) )
        producer?.flush()
    }

    /*  Função que verifica se ocorreu um evento de rua perigosa (Dangerous Road), definimos como rua perigosa qualquer rua
        que tenha gerado 3 alertas de mesmo tipo em um intervalo de 30 minutos
    */

    private fun checkCustomAlert() {
      var alert: Alerts
      val alertList = mutableListOf<Alerts>()

      for (event : Any in eventConsumed) {
          alert = jacksonObjectMapper().readValue(event as String, Alerts::class.java)

          /*Formata as datas dos alertas e remove da lista de eventos consumidos todos os alertas que aconteceram a mais de
          30 minutos
          */

          val dataMais30Min = LocalDateTime.parse(alert.publish_datetime_utc.replace("Z", "")).plusMinutes(30)
          val agora = LocalDateTime.now()

          if (agora.isAfter(dataMais30Min)) {
              eventConsumed.remove(event)
          } else {
              alertList.add(alert)
          }
      }

      /* Ordena os eventos filtrados por nome da rua, de modo que todos os elementos da mesma rua fiquem juntos */

      val sorted = alertList.sortedBy {it.street}
      var count = 0
      var stopCounting = false
      var lastValue = sorted[0].street

      /* Itera sobre o array de eventos filtrados, contando quantos eventos de cada rua foram gerados, se o contador for
       * maior que 2, produz o evento de rua perigosa, o contador reinicia sempre que um evento de rua diferente for avaliado
       * */

      for (event in sorted) {
          if (event.street == lastValue) {
              if (!stopCounting) count++
          }
          else {
               count = 1
               lastValue = event.street
               stopCounting = false
          }
          if (count > 2) {

              /* Retira os eventos da rua perigosa do array de eventos acumulados */

              eventConsumed = eventConsumed.filterNot{ it.contains(event.street)  }.toMutableList()

              val customEvent = object {
                  val street = event.street
                  val type = event.type
              }

              produce(jacksonObjectMapper().writeValueAsString(customEvent))

              count = 0
              stopCounting = true
          }

      }
    }

    /*  Função que eh executada ao iniciar o objeto, configura as propriedades do consumidor e faz o polling de eventos
     *  do topico passado como parametro
     */
    init {
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  "org.apache.kafka.common.serialization.StringDeserializer")
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        propProducer.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
        propProducer.setProperty(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer"
        )
        propProducer.setProperty(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer"
        )

        producer = KafkaProducer<String, Any>(propProducer)

        val logger: Logger = LoggerFactory.getLogger("Consumer")
        val consumer: KafkaConsumer<String, String> = KafkaConsumer<String, String>(prop)

        consumer.subscribe(listOf(topic))
        while (true) {
            val records : ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(200))
            for (record in records ) {
                logger.info(
                            "\n" + "\n" + "\n" +
                            "Key: " + record.key() + "\n" +
                            "Value: " + record.value() + "\n" +
                            "\n" + "\n" + "\n")
                eventConsumed.add(record.value())
            }

            /* Avalia a existencia de rua perigosa quando o numero de eventos simples acumulados passar de 5*/

            if (topic !== "DANGEROUS_ROAD" && eventConsumed.size > 5) {
                checkCustomAlert()
            }

            Thread.sleep(1000)

        }
    }
}


