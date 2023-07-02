package wazeApiMock
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import kotlin.random.Random

/* Classe mock representando a API do Waze */
class WazeApiMock {

    private val streets = arrayOf("Av. Anísio Fernandes Coelho", "Av. Norte Sul", "Av. Carlos Gomes de Sá", "Avenida Carlos Lindenberg","Av. Carlos Lindenberg", "Av. Américo Buaiz")
    private val types = arrayOf("JAM", "POLICE", "HAZARD", "ROAD_CLOSED")

    /* Objeto representando o retorno da requisicao da API */

    private val dataApi = object {
        val status = "200"
        val data = object {
            val alerts = mutableListOf<Alerts>()
            val jams = mutableListOf<Jams>()
        }
    }

    /* Funcao auxiliar que retorna um elemento aleatorio de um array */
    private fun getRandomElement(array: Array<String>): String {
        return array[Random.nextInt(0, array.size)]
    }

    /* Funcao que representa a chamada da api, gera n alertas diferentes de tipos e ruas aleatorios */

    fun get(): String {
        val nAlerts: Int = Random.nextInt(0, 5)
        val publishDate = LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

        repeat(nAlerts) {
            val alert = Alerts("JAM",getRandomElement(streets), publishDate)
            dataApi.data.alerts.add(alert)
        }

        if (dataApi.data.alerts.size > 100) dataApi.data.alerts.dropLast(10)

        return jacksonObjectMapper().writeValueAsString(dataApi)
    }
}