FROM confluentinc/cp-kafka-connect:7.3.2

RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:latest && \
    confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.6.4