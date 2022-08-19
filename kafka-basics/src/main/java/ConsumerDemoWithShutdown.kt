import org.apache.kafka.clients.consumer.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*


private val log : Logger = LoggerFactory.getLogger("ProducerDemo")
private val host: String = "127.0.0.1:9092"
private val groupId: String = "kotlin_fourth_group_id"
private val topic: String = "kotlin_topic"

private fun createConsumer(): Consumer<String, String> {
    val properties = Properties()
    properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = host
    properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.qualifiedName
    properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.qualifiedName
    properties[ConsumerConfig.GROUP_ID_CONFIG] = groupId
    properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

    return KafkaConsumer<String, String>(properties)
}

/* TODO - This class is not working as expected
 */
fun main(args: Array<String>) {
    log.info("Kafka Producer")

    // kafka-topics.sh --bootstrap-server localhost:9092 --create --topic kotlin_topic --partitions 3 --replication-factor 1

    val consumer = createConsumer()     // Setting up the properties

    // Get a reference to the current thread
    val mainThread: Thread = Thread.currentThread()

    // Adding a shutdown hook
    Runtime.getRuntime().addShutdownHook(Thread(){
        fun run(){
            log.info("Detected a shutdown, let's exit calling consumer.wakeup()...")
            consumer.wakeup()

            // Join the main thread to allow the execution of the code in the main thread
            try{
                mainThread.join()
            } catch (e:InterruptedException){
                e.printStackTrace()
            }
        }
    })

    try {
        consumer.subscribe(setOf(topic))                        // Subscribe consumer to a topic (you can subscribe to multiple topics)
        while (true) {
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(100))

            for (record in records) {
                log.info("Key: ${record.key()}  Value: ${record.value()}  Partition: ${record.partition()}  Offset: ${record.offset()}  ")
            }

        }
    } catch( e: WakeupException){
        log.info("Wake up exception!")
        // We ignore this as it is an expected exception when closing a consumer
    } catch( e: Error){
        log.error("Unespected exception")
    } finally {
        consumer.close()        // This will also commit the offset if need be
        log.info("Consumer is now gracefully closed")
    }
}