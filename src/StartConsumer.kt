import consumer.Consumer

/* Script para iniciar um consumidor primitivo, eh passado o topico que sera consumido como argumento pela linha de comando */
fun main(args: Array<String>) {
    Consumer(args[0], "1")
}