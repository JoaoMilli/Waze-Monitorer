package consumer

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
    private fun produce(value: Any) = run {
        val record: ProducerRecord<String, Any> = ProducerRecord<String, Any>("DANGEROUS_ROAD", value)
        producer?.send(record, Callback(
            fun(recordMetadata: RecordMetadata, e:Exception?) {
                if(e == null) {
                    logger.info("\n Key: " + record.topic() + "\n")
//                    logger.info("Topic: " + recordMetadata.topic() + "\n" +
//                            "Partition: " + recordMetadata.partition() + "\n" +
//                            "Offset: " + recordMetadata.offset() + "\n" +
//                            "Timestamp: " + recordMetadata.timestamp()
//                    )
                    logger.info("\n Data: " + record.value() + "\n")
                } else {
                    logger.info("\n Failed to produce record \n")
                }
            }
        ) )
        producer?.flush()
    }

    private fun checkCustomAlert() {
      var alert: Alerts
      val alertList = mutableListOf<Alerts>()
      for (event:Any in eventConsumed) {
          alert = jacksonObjectMapper().readValue(event as String, Alerts::class.java)
          val dataMais30Min = LocalDateTime.parse(alert.publish_datetime_utc.replace("Z", "")).plusMinutes(30)
          val agora = LocalDateTime.now()
          if (agora.isAfter(dataMais30Min)) {
              eventConsumed.remove(event)
          } else {
              alertList.add(alert)
          }
      }

        val sorted = alertList.sortedBy {it.street}
      var count = 0
        var stopCounting = false
      var lastValue = sorted[0].street
      for (event in sorted) {
          if (event.street == lastValue) {
              if(!stopCounting) count++
          }
          else {
               count = 1
               lastValue = event.street
              stopCounting = false
          }
          if (count > 2) {
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
// }
    }

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
                logger.info("Value " + record.value())
                eventConsumed.add(record.value())
            }

            if (topic !== "DANGEROUS_ROAD" && eventConsumed.size > 5) {
                checkCustomAlert()
            }

            Thread.sleep(1000)

        }
    }
}


