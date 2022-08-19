import com.launchdarkly.eventsource.EventHandler
import com.launchdarkly.eventsource.MessageEvent
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class WikimediaChangeHandle(val kafkaProducer: KafkaProducer<String, String>, val topic: String): EventHandler {

    private val log : Logger = LoggerFactory.getLogger(WikimediaChangeHandle::class.qualifiedName)

    /**
     * When stream is open
     */
    override fun onOpen() {
        log.info("Stream is open")
    }

    /**
     * When stream is closed
     */
    override fun onClosed() {
        kafkaProducer.close()
    }

    /**
     * When stream receives a message from http stream, we want to send that through kafka producer
     */
    override fun onMessage(event: String?, messageEvent: MessageEvent?) {
        log.info("Sending the message '${messageEvent!!.data}'")
        kafkaProducer.send(ProducerRecord<String, String>( topic, messageEvent!!.data))
    }

    override fun onComment(comment: String?) {
        TODO("Not yet implemented")
    }

    override fun onError(t: Throwable?) {
        log.error("Error in stream reading '${t}'")
    }
}