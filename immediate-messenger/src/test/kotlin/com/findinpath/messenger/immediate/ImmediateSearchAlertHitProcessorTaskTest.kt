package com.findinpath.messenger.immediate

import com.datastax.driver.core.Session
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
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.time.Instant
import java.util.Properties
import java.util.UUID
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors

@Testcontainers
class ImmediateSearchAlertHitProcessorTaskTest {
    private val logger = LoggerFactory.getLogger(javaClass)

    lateinit var immediateSearchAlertHitProcessorTask: ImmediateSearchAlertHitProcessorTask

    private lateinit var topic:String

    private lateinit var emailsSent : MutableList<EmailDetails>

    private lateinit var cassandraSession: Session

    private lateinit var cassandraSearchAlertRepository: CassandraSearchAlertRepository

    lateinit var immediateSearchAlertHitProducer: Producer<String, String>


    @BeforeEach
    fun setup() {
        emailsSent = mutableListOf()
        topic = "immediate-" + UUID.randomUUID().toString()

        cassandraSession = cassandraContainer.cluster.connect()
        cassandraSearchAlertRepository = CassandraSearchAlertRepository(cassandraSession)

        createTopic(topic)

        immediateSearchAlertHitProcessorTask = ImmediateSearchAlertHitProcessorTask(
            kafkaContainer.bootstrapServers,
            LoggingEmailService { emailDetails -> emailsSent.add(emailDetails) },
            cassandraSearchAlertRepository,
            topic,
            UUID.randomUUID().toString()
        )



        Executors.newCachedThreadPool().submit(immediateSearchAlertHitProcessorTask)

        await.atMost(Duration.ofSeconds(30)).until { immediateSearchAlertHitProcessorTask.partitionsAssigned }

        immediateSearchAlertHitProducer = createProducer(kafkaContainer.bootstrapServers)

        truncateCassandraTables()

    }

    @AfterEach
    fun tearDown() {
        if (this::immediateSearchAlertHitProcessorTask.isInitialized) {
            immediateSearchAlertHitProcessorTask.stop()
        }


        if (this::cassandraSession.isInitialized){
            cassandraSession.close()
        }

        immediateSearchAlertHitProducer.close(Duration.ofMillis(100))
        deleteTopic(topic)

    }

    @Test
    fun demoTest(){
        val news = News(
            1L,
            "Early snow this year",
            "After a year with hardly any snow, this is going to be a serious winter",
            "weather",
            Instant.now()
        )

        val searchAlert = SearchAlert(1L, "news aobout snow", "contact@mail.com")

        val immediateSearchAlertHit = ImmediateSearchAlertHit(searchAlert, news)

        logger.info("Write  $immediateSearchAlertHit to the topic $topic")
        immediateSearchAlertHitProducer.send(ProducerRecord(topic, searchAlert.id.toString(), jsonMapper.writeValueAsString(immediateSearchAlertHit))).get()

        await.atMost(Duration.ofSeconds(10)).until { emailsSent.size == 1 }

        assertThat(emailsSent[0].searchAlert, equalTo(searchAlert))
        assertThat(emailsSent[0].news, equalTo(news))

        assertThat(cassandraSearchAlertRepository.isProcessingDone(searchAlert.id, news.id), equalTo(true))
    }


    @Test
    fun multipleHitsForTheSameSearchAlert(){
        val news1 = News(
            1L,
            "Early snow this year",
            "After a year with hardly any snow, this is going to be a serious winter",
            "weather",
            Instant.now()
        )

        val news2 = News(
            2L,
            "Snow everywhere",
            "All the roads are filled with snow",
            "weather",
            Instant.now()
        )


        val searchAlert = SearchAlert(1L, "news aobout snow", "contact@mail.com")

        val immediateSearchAlertHit1 = ImmediateSearchAlertHit(searchAlert, news1)
        val immediateSearchAlertHit2 = ImmediateSearchAlertHit(searchAlert, news2)

        logger.info("Write  $immediateSearchAlertHit1 to the topic $topic")
        immediateSearchAlertHitProducer.send(ProducerRecord(topic, searchAlert.id.toString(), jsonMapper.writeValueAsString(immediateSearchAlertHit1))).get()
        logger.info("Write  $immediateSearchAlertHit2 to the topic $topic")
        immediateSearchAlertHitProducer.send(ProducerRecord(topic, searchAlert.id.toString(), jsonMapper.writeValueAsString(immediateSearchAlertHit2))).get()

        await.atMost(Duration.ofSeconds(10)).until { emailsSent.size == 2 }

        assertThat(emailsSent[0].searchAlert, equalTo(searchAlert))
        assertThat(emailsSent[0].news, equalTo(news1))
        assertThat(emailsSent[1].searchAlert, equalTo(searchAlert))
        assertThat(emailsSent[1].news, equalTo(news2))

        assertThat(cassandraSearchAlertRepository.isProcessingDone(searchAlert.id, news1.id), equalTo(true))
        assertThat(cassandraSearchAlertRepository.isProcessingDone(searchAlert.id, news2.id), equalTo(true))

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


    fun truncateCassandraTables(){
        cassandraContainer.cluster.connect().use { session ->
            session.execute(TRUNCATE_SEARCH_ALERT_PROCESSED_NEWS_TABLE_TABLE)
        }
    }

    companion object {
        val CONFLUENT_PLATFORM_VERSION: String = "5.5.0"


        @Container
        val kafkaContainer = KafkaContainer(CONFLUENT_PLATFORM_VERSION)


        private const val CREATE_DEMO_KEYSPACE_DDL = "CREATE KEYSPACE DEMO \n" +
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }"

        private const val CREATE_SEARCH_ALERT_PROCESSED_NEWS_TABLE_DDL =
            "CREATE TABLE DEMO.SEARCH_ALERT_PROCESSED_NEWS (\n" +
                    "search_alert_id BIGINT,\n" +
                    "news_id BIGINT,\n" +
                    "processing_date TIMESTAMP,\n" +
                    "PRIMARY KEY (search_alert_id, news_id)\n" +
                    ") WITH default_time_to_live = 259200;\n" // 3 days should be enough for keeping record of the processed news

        private const val TRUNCATE_SEARCH_ALERT_PROCESSED_NEWS_TABLE_TABLE =
            "TRUNCATE TABLE DEMO.SEARCH_ALERT_PROCESSED_NEWS"

        @Container
        val cassandraContainer = SpecifiedCassandraContainer("cassandra:3.11")

        @BeforeAll
        @JvmStatic
        internal fun setupCassandra() {
            cassandraContainer.cluster.connect().use { session ->
                session.execute(CREATE_DEMO_KEYSPACE_DDL)
                session.execute(CREATE_SEARCH_ALERT_PROCESSED_NEWS_TABLE_DDL)

            }
        }

        class SpecifiedCassandraContainer(image: String) : CassandraContainer<SpecifiedCassandraContainer>(image)

    }
}