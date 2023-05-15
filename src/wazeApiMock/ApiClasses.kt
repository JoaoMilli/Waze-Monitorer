package wazeApiMock

/* Classes auxiliares para o mock da API do Waze */
class Jams (
    val street: String,
    val block_alert_type: String,
    val publish_datetime_utc: String,
) {
}

class Alerts (
    val type: String,
    val street: String,
    val publish_datetime_utc: String,
) {
}