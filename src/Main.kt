import com.fasterxml.jackson.annotation.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.squareup.okhttp.OkHttpClient
import com.squareup.okhttp.Request
import producer.Producer
import wazeApiMock.WazeApiMock
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/* Script para iniciar os produtores, consumir da API e produzir eventos */

/* Mapeamento das classes Jams e Alerts retornados da API */
@JsonIgnoreProperties(ignoreUnknown = true)
data class Jams @JsonCreator constructor (
    @JsonProperty("street")
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    var street: String,


    @JsonProperty("block_alert_type")
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    var blockAlertType: String,

    @JsonProperty("publish_datetime_utc")
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    var publishDate: String,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class Alerts @JsonCreator constructor (
    @JsonProperty("type") var type: String,

    @JsonProperty("street")
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    var street: String,

    @JsonProperty("publish_datetime_utc")
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    var publishDate: String,
)


data class ApiData @JsonCreator constructor (
    @JsonProperty("alerts") var alerts: List<Alerts>,
    @JsonProperty("jams") var jams: List<Jams>,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class ApiRequest @JsonCreator constructor  (
    @JsonProperty("status") var status: String,
    @JsonProperty("data") var data: ApiData
)

fun main () {

    /* Define se sera usado a API verdadeira ou o mock */

    val useMockData = true

    /* Inicia produtores de eventos primitivos de cada tipo de Alerta */

    val jamProducer = Producer("JAM")
    val roadClosedProducer = Producer("ROAD_CLOSED")
    val hazardProducer = Producer("HAZARD")
    val policeProducer = Producer("POLICE")

    val mapper = jacksonObjectMapper()

    /* Define formatadores de datas para que as mesmas sejam formatadas e comparadas */

    val dataFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val apiDataFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val zoneOffset = -ZonedDateTime.now().offset.totalSeconds.toLong()

    /* Define o timestamp da ultima requisicao da API que retornou novos valores, para que sempre sejam obtidos apenas
    *  alertas novos */

    var updateAlertTimestamp = false
    var alertTimestamp = LocalDateTime.now()


    val apiMock = WazeApiMock()

    /* Monta a requisicao da API, configurando o lat lng das bordas do retangulo que contem o municipio de Vitoria */

    val client = OkHttpClient()
    val request = Request.Builder()
        .url("https://waze.p.rapidapi.com/alerts-and-jams?bottom_left=-20.320922931529864%2C%20-40.35471503733704&top_right=-20.228494261871262%2C%20-40.26442115305969&max_alerts=20&max_jams=20")
        .get()
        .addHeader("content-type", "application/octet-stream")
        .addHeader("X-RapidAPI-Key", "476ce28e4fmshc2f278f79f500f9p174118jsna096ac7dcf01")
        .addHeader("X-RapidAPI-Host", "waze.p.rapidapi.com")
        .build()

    var response: String

    while(true) {
        if (useMockData) {
            response = apiMock.get()
        } else {
            response = client.newCall(request).execute().body().string()
        }

        val data: ApiRequest = mapper.readValue(response)

        for (alert in data.data.alerts) {

            /* Verifica os alertas retornados da API, se as informacoes estiverem disponiveis e o alerta possuir timestamp
             * maior do que o timestamp do ultimo evento produzido, sera gerado um evento primitivo do tipo do alerta */

            if (alert.street !== "" && alert.type !== "" && alert.publishDate !== "") {
    
                val currentTimestamp = dataFormatter.format(alertTimestamp)
    
                val timestamp = dataFormatter.format(
                    LocalDateTime.parse(alert.publishDate, apiDataFormatter)
                        .minusSeconds(zoneOffset)
                )

                val dateCompare = timestamp.compareTo(currentTimestamp)
    
                if (dateCompare > 0) {
                    when (alert.type) {
                        "JAM" -> jamProducer.produce(jacksonObjectMapper().writeValueAsString(alert), alert.street)
                        "ROAD_CLOSED" -> roadClosedProducer.produce(jacksonObjectMapper().writeValueAsString(alert), alert.street)
                        "HAZARD" -> hazardProducer.produce(jacksonObjectMapper().writeValueAsString(alert), alert.street)
                        "POLICE" -> policeProducer.produce(jacksonObjectMapper().writeValueAsString(alert), alert.street)
                        else -> {
                            print("Evento desconhecido")
                        }
                    }

                    /* Ao produzir novos eventos, atualiza o timestamp do ultimo evento produzido */

                    updateAlertTimestamp = true
                }
    
            }
        }

        if (updateAlertTimestamp) {
            alertTimestamp = LocalDateTime.now()
        }
//
        Thread.sleep(5000)

    }

}