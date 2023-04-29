package consumer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import wazeApiMock.Alerts
import java.time.Duration
import java.time.LocalDateTime
import java.util.Properties;
class Consumer(private val topic: String, private val groupId: String) {
    private val bootstrapServer: String = "localhost:29092"
    private val prop: Properties = Properties()
    private var eventConsumed = mutableListOf<String>()

    private fun checkCustomAlert() {
      var alert: Alerts
      var alertList = mutableListOf<Alerts>()
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

        for (obj in sorted) println(obj.street)

        println()

      var count = 0
      var lastValue = sorted[0].street
      for (event in sorted) {

          if (event.street == lastValue) {
              count++
          }
          else {
               count = 1
               lastValue = event.street

          }

          if (count > 1) {
              println("COMPOSTO => " + event.street)
              for(event in eventConsumed) println("STRING => " + event.toString())
              eventConsumed = eventConsumed.filterNot{ it.contains(event.street)  }.toMutableList()

              println("NOVA LISTA => ")

              for (event in eventConsumed) println(event)

              println()

          }

      }
// }
    }

    init {
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        val logger: Logger = LoggerFactory.getLogger("Consumer");
        val consumer: KafkaConsumer<String, String> = KafkaConsumer<String, String>(prop)

        consumer.subscribe(listOf(topic))
        while (true) {
            val records : ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(200))
            for (record in records ) {
                logger.info("Value " + record.value())
                eventConsumed.add(record.value())
            }

            if (eventConsumed.size > 1) {
                checkCustomAlert()
            }

            Thread.sleep(1000)

        }
    }
}


