import com.launchdarkly.eventsource.EventHandler
import com.launchdarkly.eventsource.EventSource
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URI
import java.util.*
import java.util.concurrent.TimeUnit

private val log : Logger = LoggerFactory.getLogger("WikimediaChangesProducer")
private val host: String = "127.0.0.1:9092"
private val groupId: String = "kotlin_group_id"
private val topic: String = "wikimedia.recentchange"

private fun createProducer(): KafkaProducer<String, String> {
    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = host
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.qualifiedName
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.qualifiedName

    // Set safe producer configs (it's necessary only for Kafka <= 2.8)
    properties[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = true
    properties[ProducerConfig.ACKS_CONFIG] = "all"
    properties[ProducerConfig.RETRIES_CONFIG] = Integer.toString(Integer.MAX_VALUE)

    // Set high throughput producer configs
    properties[ProducerConfig.LINGER_MS_CONFIG] = "20"
    properties[ProducerConfig.BATCH_SIZE_CONFIG] = Integer.toString(32 * 1024)      // 32KB
    properties[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "snappy"

    return KafkaProducer<String, String>(properties)
}

fun main(args: Array<String>) {

    // kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wikimedia.recentchange --partitions 3 --replication-factor 1

    val producer: KafkaProducer<String, String> = createProducer()

    val eventHandler: EventHandler = WikimediaChangeHandle(producer, topic)
    val url: String = "https://stream.wikimedia.org/v2/stream/recentchange"
    val builder: EventSource.Builder = EventSource.Builder(eventHandler, URI.create(url))

    val eventSource: EventSource = builder.build()

    eventSource.start()     // Start the producer in another thread

    TimeUnit.MINUTES.sleep(10)
}