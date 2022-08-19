import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

private val log : Logger = LoggerFactory.getLogger("ProducerDemoWithCallback")

private fun createProducer(): Producer<String, String> {
    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.qualifiedName
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.qualifiedName
    return KafkaProducer<String, String>(properties)
}

fun main(args: Array<String>) {
    log.info("Kafka Producer using Keys")

    // kafka-topics.sh --bootstrap-server localhost:9092 --create --topic kotlin_topic --partitions 3 --replication-factor 1

    // Setting up the properties
    val producer = createProducer()

    //producer.dispatch(producerRecord)         // If you want to run using coroutine, discoment this line and add 'suspend' at the main function

    for(i in 0 .. 10){
        val topic: String = "kotlin_topic"
        val value: String = "Hello world + ${i}"
        val key:String = "id_${i}"

        val producerRecord: ProducerRecord<String, String> = ProducerRecord<String, String>("kotlin_topic", key, value)     // Create a producer record - What we are going to send in Kafka
        producer.send(producerRecord) { record: RecordMetadata, exception: Exception? ->
            if(exception == null || exception.stackTrace.isEmpty()){
                log.info("Received a new message. " +
                        "Topic '${record.topic()}, " +
                        "Key '${producerRecord.key()} " +
                        "partition: '${record.partition()}' " +
                        "offset '${record.offset()}'")
            }else
                log.error("no message received: '${exception}'")
        }
    }

    producer.flush()        // Flush data (sync) - The line below will be blocked until the data gets sent
    producer.close()        // Flush and close producer - close function already flush the data
}

private suspend inline fun <reified K : Any, reified V : Any> Producer<K, V>.dispatch(record: ProducerRecord<K, V>) =
    suspendCoroutine<RecordMetadata> { continuation ->
        val callback = Callback { metadata, exception ->
            if (metadata == null) {
                continuation.resumeWithException(exception!!)
            } else {
                continuation.resume(metadata)
            }
        }
        this.send(record, callback)
    }