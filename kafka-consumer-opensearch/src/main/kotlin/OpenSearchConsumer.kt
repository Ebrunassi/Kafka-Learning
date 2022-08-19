import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonParser
import com.google.gson.stream.JsonReader
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RequestOptions
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URI
import org.opensearch.client.RestClient
import org.opensearch.client.RestClientBuilder

import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.common.xcontent.XContentType
import java.time.Duration.*
import java.util.*

private val log : Logger = LoggerFactory.getLogger("OpenSearchConsumer")
private val host: String = "127.0.0.1:9092"
private val groupId: String = "consumer-opensearch-demo"
private val topic: String = "wikimedia.recentchange"

fun createOpenSearchClient(): RestHighLevelClient {
    val connString = "http://localhost:9200"
    //        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";
    // we build a URI from the connection string
    val restHighLevelClient: RestHighLevelClient
    val connUri: URI = URI.create(connString)
    // extract login information if it exists
    val userInfo: String? = connUri.userInfo
    if (userInfo == null) {
        // REST client without security
        restHighLevelClient =
            RestHighLevelClient(RestClient.builder(HttpHost(connUri.getHost(), connUri.getPort(), "http")))
    } else {
        // REST client with security
        val auth: Array<String> = userInfo.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        val cp: CredentialsProvider = BasicCredentialsProvider()
        cp.setCredentials(AuthScope.ANY, UsernamePasswordCredentials(auth[0], auth[1]))
        restHighLevelClient = RestHighLevelClient(
            RestClient.builder(HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                .setHttpClientConfigCallback(
                    RestClientBuilder.HttpClientConfigCallback { httpAsyncClientBuilder: HttpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                            .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy())
                    })
        )
    }
    return restHighLevelClient
}

private fun createKafkaConsumer(): KafkaConsumer<String, String> {
    val properties = Properties()
    properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = host
    properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.qualifiedName
    properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.qualifiedName
    properties[ConsumerConfig.GROUP_ID_CONFIG] = groupId
    properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
    properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"

    return KafkaConsumer<String, String>(properties)
}

fun extractId(json: String): String =
    JsonParser.parseString(json)
            .asJsonObject
            .get("meta")
            .asJsonObject
            .get("id")
            .asString

/**
 * ATTENTION! Don't forget to run docker compose file "docker compose up -d"
 * https://opensearch.org/docs/latest/
 *
 * Utils comands:
 * kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group consumer-opensearch-demo --reset-offsets --shift-by -2 --execute --all-topics     # Shift the offset by -2
 * kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group consumer-opensearch-demo --reset-offsets --to-earliest --execute --all-topics     # Reset the offset to the earliest
  */
fun main(args: Array<String>) {

    // kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wikimedia.recentchange --partitions 3 --replication-factor 1
    // First: create an OpenSearch Client
    val openSearchClient: RestHighLevelClient = createOpenSearchClient()

    // We need to create an index on OpenSearch if it doesn't exist already
    try {

        val indexExists = openSearchClient.indices().exists(GetIndexRequest("wikimedia"), RequestOptions.DEFAULT)

        if (!indexExists) {
            val createIndexRequest = CreateIndexRequest("wikimedia")
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT)
            log.info("The Wikimedia index has been created")
        } else {
            log.info("The Wikimedia index already exists")
        }
        // Second: create a Kafka Client
        val consumer: KafkaConsumer<String, String> = createKafkaConsumer()
        consumer.subscribe(Collections.singleton("wikimedia.recentchange"))         // Subscribing the consumer

        while (true){
            val records: ConsumerRecords<String, String> = consumer.poll(ofMillis(300))
            val recordCount = records.count()
            log.info("Received ${recordCount} records")

            // Let's create a Bulk Request to increase the performance to send information to OpenSearch
            val bulkRequest = BulkRequest()

            for(record in records){
                // Sent the record into OpenSearch
                // Because of the "at least once" properties, it could be possible to receive duplicated messages.
                // To deal with that and to not send duplicated data to OpenSearch, we need to make our consumer idempotent can use one of two strategies:
                // Strategy 1 - Defining a unique ID
                try {
                    val recordIdStrategy1: String = record.topic() + "_" + record.partition() + "_" + record.offset()            // We can assure that this ID is unique

                    // Strategy 2 - If the record already provides an ID, so use it!
                    val recordIdStrategy2 = extractId(record.value())

                    val indexRequest: IndexRequest = IndexRequest("wikimedia")
                        .source(record.value(), XContentType.JSON)
                        .id(recordIdStrategy2)                                                   // If we follow the strategy 1, this line is necessary

                    bulkRequest.add(indexRequest)

                    //val response: IndexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT)        // These lines has been commented because we are going to send the data via bulk request
                    //log.info("Inserted 1 document into OpenSearch. Id '${response.id}'")
                } catch (e: Exception){
                    log.error("Error catched: ${e.message}")
                }
            }

            if (bulkRequest.numberOfActions() > 0){
                val bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT)
                log.info("Inserted ${bulkResponse.items.size} records")
            }

            try{
                Thread.sleep(1000)
            } catch ( e: Exception ){
                log.error(e.message)
            }
            // Commit offset after batch is consumed
            consumer.commitSync()
            log.info("Offsets have been commited")

        }
        // Main code logic

        // Close things
    } catch(e: Throwable){
        log.error(e.message)
    } finally {
        openSearchClient.close()
    }
}