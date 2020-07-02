package com.findinpath.messenger.batch

import com.datastax.driver.core.Session
import com.findinpath.messenger.batch.BatchedSearchAgentHitProcessorTask.Companion.NEWS_INDEX
import com.findinpath.messenger.batch.BatchedSearchAgentHitProcessorTask.Companion.NEWS_NOTIFY_INDEX
import org.apache.http.HttpHost
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.serialization.StringSerializer
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
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.Matchers.greaterThan
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.CassandraContainer
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
class BatchedSearchAgentHitProcessorTaskTest {

    private val logger = LoggerFactory.getLogger(javaClass)


    private lateinit var topic:String

    private lateinit var emailsSent : MutableList<EmailDetails>

    private lateinit var batchedSearchAgentHitProducer: Producer<String, String>

    private lateinit var batchedSearchAgentHitProcessorTask: BatchedSearchAgentHitProcessorTask

    private lateinit var cassandraSession: Session

    private lateinit var cassandraSearchAgentRepository: CassandraSearchAgentRepository

    private val frequency = Frequency.HOURLY

    private val lastHourFrequencyWindow = Instant.now()
        .minus(1, ChronoUnit.HOURS)
        .truncatedTo(ChronoUnit.HOURS)

    @BeforeEach
    fun setup() {
        emailsSent = mutableListOf()
        topic = "batched-" + UUID.randomUUID().toString()

        cassandraSession = cassandraContainer.cluster.connect()
        cassandraSearchAgentRepository = CassandraSearchAgentRepository(cassandraSession)



        batchedSearchAgentHitProcessorTask = BatchedSearchAgentHitProcessorTask(
            kafkaContainer.bootstrapServers,
            listOf(HttpHost.create(elasticsearchContainer.httpHostAddress)),
            cassandraSearchAgentRepository,
            LoggingEmailService { emailDetails -> emailsSent.add(emailDetails) },
            topic,
            frequency,
            lastHourFrequencyWindow,
            UUID.randomUUID().toString()
        )


        createTopic(topic)

        batchedSearchAgentHitProducer = createProducer(kafkaContainer.bootstrapServers)

        getClient(elasticsearchContainer).use { client ->
            createNewsIndex(client)
            createNewsNotifyIndex(client)
        }

    }

    @AfterEach
    fun tearDown() {
        if (this::batchedSearchAgentHitProcessorTask.isInitialized) {
            batchedSearchAgentHitProcessorTask.stop()
        }

        if (this::cassandraSession.isInitialized){
            cassandraSession.close()
        }

        batchedSearchAgentHitProducer.close(Duration.ofMillis(100))
        deleteTopic(topic)

    }


    @Test
    fun stopOnEmptyTopicAccuracy(){

        startBatchedSearchAgentHitProcessorTask()

        truncateCassandraTables()

        await.atMost(Duration.ofSeconds(10))
            .until { batchedSearchAgentHitProcessorTask.isStopping() }
    }


    @Test
    fun noEmailsShouldBeSentWhenTheSearchAgentHasBeenRemovedFromTheNewsNotifyIndex(){
        val newsId = 1L
        val searchAgentId = 1L

        val batchedSearchAgentHit = BatchedSearchAgentHit(searchAgentId, newsId)

        logger.info("Write  $batchedSearchAgentHit to the topic $topic")
        batchedSearchAgentHitProducer
            .send(ProducerRecord(topic, searchAgentId.toString(), jsonMapper.writeValueAsString(batchedSearchAgentHit)))
            .get()

        startBatchedSearchAgentHitProcessorTask()

        await.atMost(Duration.ofSeconds(10))
            .until { batchedSearchAgentHitProcessorTask.isStopping() }

        assertThat(emailsSent.size, equalTo(0))
    }


    @Test
    fun noEmailsShouldBeSentWhenTheSearchAgentHasNoHits(){
        val newsId = 1L
        val searchAgentId = 1L

        getClient(elasticsearchContainer).use { client ->
            indexNewsNotifyDocument(client, searchAgentId, "news about snow", "contact@mail.com", Frequency.DAILY, "snow", "weather")
        }

        val batchedSearchAgentHit = BatchedSearchAgentHit(searchAgentId, newsId)

        logger.info("Write  $batchedSearchAgentHit to the topic $topic")
        batchedSearchAgentHitProducer
            .send(ProducerRecord(topic, searchAgentId.toString(), jsonMapper.writeValueAsString(batchedSearchAgentHit)))
            .get()

        startBatchedSearchAgentHitProcessorTask()

        await.atMost(Duration.ofSeconds(10))
            .until { batchedSearchAgentHitProcessorTask.isStopping() }

        assertThat(emailsSent.size, equalTo(0))
    }

    @Test
    fun emailShouldBeSentWhenTheSearchAgentHasHits(){
        val newsId = 1L
        val searchAgentId = 1L

        val testStartInstant = Instant.now()

        val news = News(
            newsId,
            "Early snow this year",
            "After a year with hardly any snow, this is going to be a serious winter",
            "weather",
            lastHourFrequencyWindow.plus(10, ChronoUnit.MINUTES)
        )


        getClient(elasticsearchContainer).use { client ->
            indexNewsDocument(client, news)
            indexNewsNotifyDocument(client, searchAgentId, "news about snow", "contact@mail.com", Frequency.DAILY, "snow", "weather")
        }

        val batchedSearchAgentHit = BatchedSearchAgentHit(searchAgentId, newsId)

        logger.info("Write  $batchedSearchAgentHit to the topic $topic")
        batchedSearchAgentHitProducer
            .send(ProducerRecord(topic, searchAgentId.toString(), jsonMapper.writeValueAsString(batchedSearchAgentHit)))
            .get()

        startBatchedSearchAgentHitProcessorTask()

        await.atMost(Duration.ofSeconds(10))
            .until { batchedSearchAgentHitProcessorTask.isStopping() }

        assertThat(emailsSent.size, equalTo(1))
        assertThat(emailsSent[0].searchAgent.id, equalTo(searchAgentId))
        assertThat(emailsSent[0].newsList.size, equalTo(1))
        assertThat(emailsSent[0].newsList[0].id, equalTo(newsId))

        val processingWindow = cassandraSearchAgentRepository.getLastSearchAgentHitProcessingWindow(searchAgentId, frequency)
        assertThat(processingWindow, equalTo(lastHourFrequencyWindow))

        val lastPublishedDate = cassandraSearchAgentRepository.getSearchAgentLastPublishedDate(searchAgentId)
        assertThat(lastPublishedDate, greaterThan(testStartInstant))
    }


    @Test
    fun batchEmailShouldBeSentWhenTheSearchAgentHasHits(){
        val news1Id = 1L
        val news2Id = 2L
        val searchAgentId = 1L

        val testStartInstant = Instant.now()

        val news1 = News(
            news1Id,
            "Early snow this year",
            "After a year with hardly any snow, this is going to be a serious winter",
            "weather",
            lastHourFrequencyWindow.plus(10, ChronoUnit.MINUTES)
        )

        val news2  = News(
            news2Id,
            "Snow on the ground, sun in the sky",
            "I am waiting for the day where kids can skate on the water and the dog can play in the snow while we are sitting in the sun.",
            "weather",
            lastHourFrequencyWindow.plus(20, ChronoUnit.MINUTES)
        )


        getClient(elasticsearchContainer).use { client ->
            indexNewsDocument(client, news1)
            indexNewsDocument(client, news2)
            indexNewsNotifyDocument(client, searchAgentId, "news about snow", "contact@mail.com", Frequency.DAILY, "snow", "weather")
        }

        val batchedSearchAgentHit = BatchedSearchAgentHit(searchAgentId, news1Id)

        logger.info("Write  $batchedSearchAgentHit to the topic $topic")
        batchedSearchAgentHitProducer
            .send(ProducerRecord(topic, searchAgentId.toString(), jsonMapper.writeValueAsString(batchedSearchAgentHit)))
            .get()

        startBatchedSearchAgentHitProcessorTask()

        await.atMost(Duration.ofSeconds(10))
            .until { batchedSearchAgentHitProcessorTask.isStopping() }

        assertThat(emailsSent.size, equalTo(1))
        assertThat(emailsSent[0].searchAgent.id, equalTo(searchAgentId))
        assertThat(emailsSent[0].newsList.size, equalTo(2))
        assertThat(emailsSent[0].newsList.map(News::id), containsInAnyOrder(news1Id, news2Id))

        val processingWindow = cassandraSearchAgentRepository.getLastSearchAgentHitProcessingWindow(searchAgentId, frequency)
        assertThat(processingWindow, equalTo(lastHourFrequencyWindow))

        val lastPublishedDate = cassandraSearchAgentRepository.getSearchAgentLastPublishedDate(searchAgentId)
        assertThat(lastPublishedDate, greaterThan(testStartInstant))
    }



    @Test
    fun noDuplicateEmailsShouldBeSentWhenForBatchedSearchHitsBelongingToTheSameSearchAgent(){
        val news1Id = 1L
        val news2Id = 2L
        val searchAgentId = 1L

        val testStartInstant = Instant.now()

        val news1 = News(
            news1Id,
            "Early snow this year",
            "After a year with hardly any snow, this is going to be a serious winter",
            "weather",
            lastHourFrequencyWindow.plus(10, ChronoUnit.MINUTES)
        )

        val news2  = News(
            news2Id,
            "Snow on the ground, sun in the sky",
            "I am waiting for the day where kids can skate on the water and the dog can play in the snow while we are sitting in the sun.",
            "weather",
            lastHourFrequencyWindow.plus(20, ChronoUnit.MINUTES)
        )


        getClient(elasticsearchContainer).use { client ->
            indexNewsDocument(client, news1)
            indexNewsDocument(client, news2)
            indexNewsNotifyDocument(client, searchAgentId, "news about snow", "contact@mail.com", Frequency.DAILY, "snow", "weather")
        }

        val batchedSearchAgentHit1 = BatchedSearchAgentHit(searchAgentId, news1Id)
        val batchedSearchAgentHit2 = BatchedSearchAgentHit(searchAgentId, news2Id)

        logger.info("Write  $batchedSearchAgentHit1 and $batchedSearchAgentHit2 to the topic $topic")
        // By using the same partition key (the search agent id) we can ensure that the eventual multipl
        // batched search agent hits for a search agent are processed sequentially
        batchedSearchAgentHitProducer
            .send(ProducerRecord(topic, searchAgentId.toString(), jsonMapper.writeValueAsString(batchedSearchAgentHit1)))
            .get()
        batchedSearchAgentHitProducer
            .send(ProducerRecord(topic, searchAgentId.toString(), jsonMapper.writeValueAsString(batchedSearchAgentHit2)))
            .get()

        startBatchedSearchAgentHitProcessorTask()

        await.atMost(Duration.ofSeconds(10))
            .until { batchedSearchAgentHitProcessorTask.isStopping() }

        assertThat(emailsSent.size, equalTo(1))
        assertThat(emailsSent[0].searchAgent.id, equalTo(searchAgentId))
        assertThat(emailsSent[0].newsList.size, equalTo(2))
        assertThat(emailsSent[0].newsList.map(News::id), containsInAnyOrder(news1Id, news2Id))

        val processingWindow = cassandraSearchAgentRepository.getLastSearchAgentHitProcessingWindow(searchAgentId, frequency)
        assertThat(processingWindow, equalTo(lastHourFrequencyWindow))

        val lastPublishedDate = cassandraSearchAgentRepository.getSearchAgentLastPublishedDate(searchAgentId)
        assertThat(lastPublishedDate, greaterThan(testStartInstant))
    }


    private fun startBatchedSearchAgentHitProcessorTask(){
        Executors.newCachedThreadPool().submit(batchedSearchAgentHitProcessorTask)
        await.atMost(Duration.ofSeconds(30)).until { batchedSearchAgentHitProcessorTask.partitionsAssigned }
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


    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = brokers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        return KafkaProducer<String, String>(props)
    }


    private fun getClient(container: ElasticsearchContainer): RestHighLevelClient {
        return RestHighLevelClient(RestClient.builder(HttpHost.create(container.httpHostAddress)))
    }

    private fun createNewsIndex(client: RestHighLevelClient) {
        if (client.indices().exists(GetIndexRequest(NEWS_INDEX), RequestOptions.DEFAULT)) {
            client.indices().delete(DeleteIndexRequest(NEWS_INDEX), RequestOptions.DEFAULT)
        }
        val createNewsIndexRequest = CreateIndexRequest(NEWS_INDEX)
        val newsMappingsbuilder = XContentFactory.jsonBuilder()
        newsMappingsbuilder.startObject().apply {
            startObject("properties").apply {
                startObject("title").apply {
                    field("type", "text")
                }.endObject()
                startObject("body").apply {
                    field("type", "text")
                }.endObject()
                startObject("category").apply {
                    field("type", "keyword")
                }.endObject()
                startObject("published_date").apply {
                    field("type", "date_nanos")
                }.endObject()
            }.endObject()
        }.endObject()
        createNewsIndexRequest.mapping(newsMappingsbuilder)
        client.indices().create(createNewsIndexRequest, RequestOptions.DEFAULT)
    }


    private fun createNewsNotifyIndex(client: RestHighLevelClient) {
        if (client.indices().exists(GetIndexRequest(NEWS_NOTIFY_INDEX), RequestOptions.DEFAULT)) {
            client.indices().delete(DeleteIndexRequest(NEWS_NOTIFY_INDEX), RequestOptions.DEFAULT)
        }
        val createNewsNotifyIndexRequest = CreateIndexRequest(NEWS_NOTIFY_INDEX)
        val newsNotifyMappingsbuilder = XContentFactory.jsonBuilder()
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
                startObject("name").apply {
                    field("type", "text")
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


    private fun indexNewsDocument(
        client: RestHighLevelClient,
        news: News
    ) {


        val sourcebuilder = XContentFactory.jsonBuilder()
        sourcebuilder.startObject().apply {
            field("title", news.title)
            field("body", news.body)
            field("category", news.category)
            field("published_date", news.publishedDate)
        }.endObject()
        val request = IndexRequest(NEWS_INDEX)
            .id(news.id.toString())
            .source(sourcebuilder)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)

        client.index(request, RequestOptions.DEFAULT)
    }

    private fun indexNewsNotifyDocument(
        client: RestHighLevelClient,
        id: Long,
        name: String,
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
            field("name", name)
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

    fun truncateCassandraTables(){
        cassandraContainer.cluster.connect().use { session ->
            session.execute(TRUNCATE_SEARCH_AGENT_LAST_PROCESSING_WINDOW_TABLE)
            session.execute(TRUNCATE_SEARCH_AGENT_LAST_PUBLISHED_DATE_TABLE)
        }
    }


    companion object {
        private const val CONFLUENT_PLATFORM_VERSION: String = "5.5.0"

        private const val CREATE_DEMO_KEYSPACE_DDL = "CREATE KEYSPACE DEMO \n" +
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }"

        private const val CREATE_SEARCH_AGENT_LAST_PROCESSING_WINDOW_TABLE_DDL =
            "CREATE TABLE DEMO.SEARCH_AGENT_LAST_PROCESSING_WINDOW (\n" +
                    "id BIGINT,\n" +
                    "frequency VARCHAR,\n" +
                    "window TIMESTAMP,\n" +
                    "PRIMARY KEY (id, frequency)\n" +
                    ")\n"

        private const val TRUNCATE_SEARCH_AGENT_LAST_PROCESSING_WINDOW_TABLE =
            "TRUNCATE TABLE DEMO.SEARCH_AGENT_LAST_PROCESSING_WINDOW"

        private const val CREATE_SEARCH_AGENT_LAST_PUBLISHED_DATE_TABLE_DDL =
            "CREATE TABLE DEMO.SEARCH_AGENT_LAST_PUBLISHED_DATE (\n" +
                    "id BIGINT,\n" +
                    "last_published_date TIMESTAMP,\n" +
                    "PRIMARY KEY (id)\n" +
                    ")\n"
        private const val TRUNCATE_SEARCH_AGENT_LAST_PUBLISHED_DATE_TABLE =
            "TRUNCATE TABLE DEMO.SEARCH_AGENT_LAST_PUBLISHED_DATE"

        /**
         * Elasticsearch version which should be used for the Tests
         */
        private val ELASTICSEARCH_VERSION: String = Version.CURRENT.toString()

        @Container
        val elasticsearchContainer =
            ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:$ELASTICSEARCH_VERSION")

        @Container
        val kafkaContainer = KafkaContainer(CONFLUENT_PLATFORM_VERSION)

        @Container
        val cassandraContainer = SpecifiedCassandraContainer("cassandra:3.11")

        @BeforeAll
        @JvmStatic
        internal fun setupCassandra() {
            cassandraContainer.cluster.connect().use { session ->
                session.execute(CREATE_DEMO_KEYSPACE_DDL)
                session.execute(CREATE_SEARCH_AGENT_LAST_PROCESSING_WINDOW_TABLE_DDL)
                session.execute(CREATE_SEARCH_AGENT_LAST_PUBLISHED_DATE_TABLE_DDL)

            }
        }
    }

    class SpecifiedCassandraContainer(image: String) : CassandraContainer<SpecifiedCassandraContainer>(image)
}