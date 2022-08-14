import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties

private val log : Logger = LoggerFactory.getLogger("ProducerDemo")

private fun createProducer(): Producer<String, String> {
    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.qualifiedName
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.qualifiedName
    return KafkaProducer<String, String>(properties)
}

fun main(args: Array<String>) {
    log.info("Kafka Producer")

    // Setting up the properties
    val producer = createProducer()

    // Create a producer record - What we are going to send in Kafka
    val producerRecord: ProducerRecord<String, String> = ProducerRecord<String, String>("kotlin_topic", "First message sent by Kafka producer")

    // Send the data async
    producer.send(producerRecord)

    // Flush data (sync) - The line below will be blocked until the data gets sent
    producer.flush()

    // Flush and close producer - close function already flush the data
    producer.close()
}
