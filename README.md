# Waze-Monitorer
Monitoramento da API do Waze para geração de eventos utilizando Apache Kafka


# Criar Tópico

bash kafka-topics.sh --bootstrap-server localhost:9092 --topic alerts --create

# Iniciar Servidor

bash kafka-server-start.sh ../config/server.properties

# Iniciar Zookeeper

bash zookeeper-server-start.sh ../config/zookeeper.properties
