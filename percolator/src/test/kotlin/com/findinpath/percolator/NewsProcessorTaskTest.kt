package com.findinpath.percolator

import org.apache.http.HttpHost
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KeyValue
import org.awaitility.kotlin.await
import org.elasticsearch.Version
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Properties
import java.util.UUID
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors

@Testcontainers
class NewsProcessorTaskTest {
    private val logger = LoggerFactory.getLogger(javaClass)

    lateinit var newsProcessorTask: NewsProcessorTask

    lateinit var newsProducer: Producer<String, String>

    val start = Instant.now()
    val next1HourTopic =getNextHourTopic(start)
    val next2HourTopic = getNextHourTopic(start.plus(1, ChronoUnit.HOURS))
    val next1DayTopic = getNextDayTopic(start)
    val next2DayTopic = getNextDayTopic(start.plus(1, ChronoUnit.DAYS))


    @BeforeEach
    fun setup() {
        newsProcessorTask = NewsProcessorTask(
            kafkaContainer.bootstrapServers, listOf(
                HttpHost.create(elasticsearchContainer.httpHostAddress)
            ),
            UUID.randomUUID().toString()
        )


        createTopic(newsTopic)
        createTopic(immediateTopic)
        createTopic(next1HourTopic)
        createTopic(next2HourTopic)

        createTopic(next1DayTopic)
        createTopic(next2DayTopic)

        Executors.newCachedThreadPool().submit(newsProcessorTask)

        await.atMost(Duration.ofSeconds(30)).until { newsProcessorTask.partitionsAssigned }

        newsProducer = createProducer(kafkaContainer.bootstrapServers)

        getClient(elasticsearchContainer).use { client ->
            createNewsNotifyIndex(client)
        }

    }

    @AfterEach
    fun tearDown() {
        if (this::newsProcessorTask.isInitialized) {
            newsProcessorTask.stop()
        }

        newsProducer.close(Duration.ofMillis(100))
        deleteTopic(newsTopic)
        deleteTopic(immediateTopic)
        deleteTopic(next1HourTopic)
        deleteTopic(next2HourTopic)
        deleteTopic(next1DayTopic)
        deleteTopic(next2DayTopic)

    }

    @Test
    fun immediateSearchAlertNotificationDemo() {
        val searchAlertId = 1L
        val searchAlertEmail = "contact@mail.com"
        getClient(elasticsearchContainer).use { client ->
            indexNewsNotifyDocument(client, searchAlertId, searchAlertEmail, Frequency.IMMEDIATE, "snow", "weather")
        }

        val news = News(
            1L,
            "Early snow this year",
            "After a year with hardly any snow, this is going to be a serious winter",
            "weather",
            Instant.now()
        )

        logger.info("Write news ${news.id} to $newsTopic")
        newsProducer.send(ProducerRecord(newsTopic, jsonMapper.writeValueAsString(news))).get()


        val searchAlertHits = dumpTopics(kafkaContainer.bootstrapServers, listOf("immediate"), 1)
        println(searchAlertHits)
        assertThat(searchAlertHits.size, equalTo(1))
        assertThat(searchAlertHits[0].key, equalTo(searchAlertId.toString()))
        val searchAlertHit = jsonMapper.readValue(searchAlertHits[0].value, SearchAlertHit::class.java)
        assertThat(searchAlertHit, equalTo(SearchAlertHit(news.id, searchAlertId, searchAlertEmail, Frequency.IMMEDIATE, searchAlertHit.searchAlertQuery, searchAlertHit.instant)))
    }


    @Test
    fun immediateSearchAlertNotificationMultipleHits() {
        val searchAlert1Id = 1L
        val searchAlert1Email = "contact@mail1.com"
        val searchAlert2Id = 2L
        val searchAlert2Email = "contact@mail2.com"
        getClient(elasticsearchContainer).use { client ->
            indexNewsNotifyDocument(client, searchAlert1Id, searchAlert1Email, Frequency.IMMEDIATE, "snow", "weather")
            indexNewsNotifyDocument(client, searchAlert2Id, searchAlert2Email, Frequency.IMMEDIATE, "snow", "weather")
        }

        val news = News(
            1L,
            "Early snow this year",
            "After a year with hardly any snow, this is going to be a serious winter",
            "weather",
            Instant.now()
        )

        logger.info("Write news ${news.id} to $newsTopic")
        newsProducer.send(ProducerRecord(newsTopic, jsonMapper.writeValueAsString(news))).get()


        val searchAlertHits = dumpTopics(kafkaContainer.bootstrapServers, listOf("immediate"), 2)
        println(searchAlertHits)
        assertThat(searchAlertHits.size, equalTo(2))
        assertThat(searchAlertHits.map(KeyValue<String, String>::key), equalTo(listOf(searchAlert1Id.toString(), searchAlert2Id.toString())))
    }

    @Test
    fun hourlySearchAlertNotificationDemo() {
        getClient(elasticsearchContainer).use { client ->
            indexNewsNotifyDocument(client, 1L, "contact@mail.com",  Frequency.HOURLY, "snow", "weather")
        }

        val news = News(
            1L,
            "Early snow this year",
            "After a year with hardly any snow, this is going to be a serious winter",
            "weather",
            Instant.now()
        )

        logger.info("Write news ${news.id} to $newsTopic")
        newsProducer.send(ProducerRecord(newsTopic, jsonMapper.writeValueAsString(news))).get()


        val searchAlertHits = dumpTopics(kafkaContainer.bootstrapServers, listOf(next1HourTopic, next2HourTopic), 1)
        println(searchAlertHits)
        assertThat(searchAlertHits.size, equalTo(1))
    }


    @Test
    fun dailySearchAlertNotificationDemo() {
        getClient(elasticsearchContainer).use { client ->
            indexNewsNotifyDocument(client, 1L, "contact@mail.com", Frequency.DAILY, "snow", "weather")
        }

        val news = News(
            1L,
            "Early snow this year",
            "After a year with hardly any snow, this is going to be a serious winter",
            "weather",
            Instant.now()
        )

        logger.info("Write news ${news.id} to $newsTopic")
        newsProducer.send(ProducerRecord(newsTopic, jsonMapper.writeValueAsString(news))).get()


        val searchAlertHits = dumpTopics(kafkaContainer.bootstrapServers, listOf(next1DayTopic, next2DayTopic), 1)
        println(searchAlertHits)
        assertThat(searchAlertHits.size, equalTo(1))
    }


    private fun getNextHourTopic(instant: Instant): String {
        return "hour-" + instant.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS).toEpochMilli()
    }

    private fun getNextDayTopic(instant: Instant): String {
        return "day-" + instant.truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.HOURS).toEpochMilli()
    }

    private fun getClient(container: ElasticsearchContainer): RestHighLevelClient {
        return RestHighLevelClient(RestClient.builder(HttpHost.create(container.httpHostAddress)))
    }

    private fun indexNewsNotifyDocument(
        client: RestHighLevelClient,
        id: Long,
        email: String,
        frequency: Frequency,
        title: String,
        category: String
    ) {

        val qb: QueryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("title", title))
            .filter(QueryBuilders.termQuery("category", category))

        val sourcebuilder = XContentFactory.jsonBuilder()
        sourcebuilder.startObject().apply {
            field("email", email)
            field("query", qb)
            field("tie_breaker_id", id)
            field("frequency", frequency.toString())
        }.endObject()
        val request = IndexRequest(NEWS_NOTIFY_INDEX)
            .id(id.toString())
            .source(sourcebuilder)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)

        client.index(request, RequestOptions.DEFAULT)
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic The name of the topic.
     * @param partitions The number of partitions for this topic.
     * @param replication The replication factor for (partitions of) this topic.
     * @param topicConfig Additional topic-level configuration settings.
     */
    @JvmOverloads
    fun createTopic(
        topic: String,
        partitions: Int = 1,
        replication: Short = 1.toShort(),
        topicConfig: Map<String, String> = emptyMap()
    ) {
        logger.info(
            "Creating topic { name: $topic, partitions: $partitions, replication: $replication, config: $topicConfig }"
        )

        val properties = Properties()
        properties[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.bootstrapServers

        try {
            AdminClient.create(properties).use { adminClient ->
                val newTopic = NewTopic(topic, partitions, replication)
                newTopic.configs(topicConfig)
                adminClient.createTopics(setOf(newTopic)).all().get()
            }
        } catch (fatal: InterruptedException) {
            throw RuntimeException(fatal)
        } catch (fatal: ExecutionException) {
            throw RuntimeException(fatal)
        }
    }

    /**
     * Delete a Kafka topic.
     *
     * @param topic The name of the topic.
     */
    fun deleteTopic(topic: String) {
        logger.info("Deleting topic $topic")
        val properties = Properties()
        properties[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.bootstrapServers

        try {
            AdminClient.create(properties).use { adminClient -> adminClient.deleteTopics(setOf(topic)).all().get() }
        } catch (e: InterruptedException) {
            throw RuntimeException(e)
        } catch (e: ExecutionException) {
            if (e.cause !is UnknownTopicOrPartitionException) {
                throw RuntimeException(e)
            }
        }
    }


    private fun createConsumer(brokers: String, consumerGroupId: String): Consumer<String, String> {
        val props = Properties()
        props[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = brokers
        props[ConsumerConfig.GROUP_ID_CONFIG] = consumerGroupId
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return KafkaConsumer<String, String>(props)
    }

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = brokers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        return KafkaProducer<String, String>(props)
    }


    fun dumpTopics(
        brokers: String,
        topics: List<String>,
        minCount: Int = 1
    ): List<KeyValue<String, String>> {
        return createConsumer(
            brokers,
            UUID.randomUUID().toString()
        ).use(fun(it: Consumer<String, String>): List<KeyValue<String, String>> {
            return it.waitFor(topics, minCount)
                .map { KeyValue(it.key(), it.value()) }
                .toList()
        })
    }

    private fun createNewsNotifyIndex(client: RestHighLevelClient) {
        if (client.indices().exists(GetIndexRequest(NEWS_NOTIFY_INDEX), RequestOptions.DEFAULT)) {
            client.indices().delete(DeleteIndexRequest(NEWS_NOTIFY_INDEX), RequestOptions.DEFAULT)
        }
        val createNewsNotifyIndexRequest = CreateIndexRequest(NEWS_NOTIFY_INDEX)
        val newsNotifyMappingsbuilder = jsonBuilder()
        newsNotifyMappingsbuilder.startObject().apply {
            startObject("properties").apply {
                startObject("title").apply {
                    field("type", "text")
                }.endObject()
                startObject("category").apply {
                    field("type", "keyword")
                }.endObject()
                startObject("tie_breaker_id").apply {
                    field("type", "long")
                }.endObject()
                startObject("email").apply {
                    field("type", "text")
                }.endObject()
                startObject("frequency").apply {
                    field("type", "keyword")
                }.endObject()
                startObject("query").apply {
                    field("type", "percolator")
                }.endObject()
            }.endObject()
        }.endObject()
        createNewsNotifyIndexRequest.mapping(newsNotifyMappingsbuilder)
        client.indices().create(createNewsNotifyIndexRequest, RequestOptions.DEFAULT)
    }


    companion object {
        /**
         * Elasticsearch version which should be used for the Tests
         */
        val ELASTICSEARCH_VERSION: String = Version.CURRENT.toString()
        val NEWS_NOTIFY_INDEX: String = "news-notify"

        @Container
        val elasticsearchContainer =
            ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:$ELASTICSEARCH_VERSION")

        val CONFLUENT_PLATFORM_VERSION: String = "5.5.0"


        @Container
        val kafkaContainer = KafkaContainer(CONFLUENT_PLATFORM_VERSION)
    }
}