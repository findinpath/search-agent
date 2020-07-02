package com.findinpath.messenger.batch

import org.apache.http.HttpHost
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.index.query.AbstractQueryBuilder
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders.rangeQuery
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.SearchModule
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.FieldSortBuilder
import org.elasticsearch.search.sort.SortOrder
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.Date
import java.util.Properties

/**
 *
 * @param frequency the frequency for the search agents (e.g. : hourly, daily)
 * @param frequencyWindow the begin of the frequency window (hour/day)
 */
class BatchedSearchAgentHitProcessorTask(
    kafkaBootstrapServers: String,
    elasticHosts: List<HttpHost>,
    val searchAgentRepository: SearchAgentRepository,
    val emailService: EmailService,
    val topic: String,
    val frequency: Frequency,
    val frequencyWindow: Instant,
    searchAgentHitConsumerGroupId: String = CONSUMER_GROUP_ID
) : Runnable {

    private val logger = LoggerFactory.getLogger(javaClass)


    private val searchAgentHitConsumer = createConsumer(kafkaBootstrapServers, searchAgentHitConsumerGroupId)

    val elasticClient = RestHighLevelClient(RestClient.builder(*elasticHosts.toTypedArray()))


    private var stopping: Boolean = false
    var partitionsAssigned: Boolean = false
        get() = field


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


    override fun run() {
        searchAgentHitConsumer.subscribe(listOf(topic), object : ConsumerRebalanceListener {
            override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {
                logger.info("Partitions ${partitions} for the topic ${topic} were assigned on the search agent hit task")
                partitionsAssigned = true
            }

            override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
            }
        })


        while (!stopping) {
            val records = searchAgentHitConsumer.poll(Duration.ofSeconds(1))
            logger.info("Received ${records.count()} search agent hit records")


            if (records.isEmpty) {
                val topicPartitions = searchAgentHitConsumer.assignment()
                val endOffsets = searchAgentHitConsumer.endOffsets(topicPartitions)
                val currentOffsets = topicPartitions.map { it to searchAgentHitConsumer.position(it) }.toMap()

                val isConsumerAndTheEndOfTheTopic = endOffsets.entries
                    .all { (topicPartition, endOffset) -> endOffset == currentOffsets.get(topicPartition) }

                if (isConsumerAndTheEndOfTheTopic) {
                    logger.info("Reached the end of the topic for the assigned partitions. Stopping ...")
                    stop()
                    break
                }
            }

            records.iterator().forEach {
                val searchAgentHitJson = it.value()
                var searchAgentHit: BatchedSearchAgentHit? = null
                try {
                    searchAgentHit = jsonMapper.readValue(searchAgentHitJson, BatchedSearchAgentHit::class.java)
                } catch (e: Exception) {
                    logger.error("Exception occurred while deserializing JSON content $searchAgentHitJson", e)

                }
                if (searchAgentHit != null) {
                    try {
                        process(searchAgentHit)
                    } catch (e: Exception) {
                        logger.error("Exception occurred while processing search agent hit $searchAgentHit", e)
                    }
                }
            }

            try {
                searchAgentHitConsumer.commitSync();
            } catch (e: CommitFailedException) {
                logger.error("Commit of the Apache Kafka offset failed", e)
            }
        }

    }

    fun process(searchAgentHit: BatchedSearchAgentHit) {
        val searchAgentId = searchAgentHit.searchAgentId
        val lastProcessedFrequencyWindow =
            searchAgentRepository.getLastSearchAgentHitProcessingWindow(searchAgentId, frequency)

        if (lastProcessedFrequencyWindow != null &&
            (lastProcessedFrequencyWindow.equals(frequencyWindow) || lastProcessedFrequencyWindow.isAfter(
                frequencyWindow
            ))
        ) {
            // avoid sending duplicate emails for the search agent in this frequency window.
            return
        }

        // retrieve from elastic the search agent details
        val searchAgentGetRequest = GetRequest("news-notify", searchAgentHit.searchAgentId.toString())
        val searchAgentGetResponse = elasticClient.get(searchAgentGetRequest, RequestOptions.DEFAULT)
        if (!searchAgentGetResponse.isExists){
            logger.info("The search agent ${searchAgentId} has been removed from the $NEWS_NOTIFY_INDEX index so no further processing needed.")
            return
        }

        val searchAgent = createSearchAgent(searchAgentGetResponse)


        // retrieve the actual news for the search agent configured query
        val source = searchAgentGetResponse.sourceAsString
        val sourceJson = JSONObject(source)
        val searchAgentQueryBuilder = parseQuery(sourceJson.get("query").toString())

        val lastSearchedPublishedDate = searchAgentRepository
            .getSearchAgentLastPublishedDate(searchAgentHit.searchAgentId)
            ?: frequencyWindow

        // add published date filter with latest search date retrieved from Cassandra
        // we assume that all the search agents are configured as boolean queries
        // see https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html
        (searchAgentQueryBuilder as BoolQueryBuilder)
            .filter(rangeQuery("published_date").gt(Date.from(lastSearchedPublishedDate)))

        val newsSearchRequest = SearchRequest("news")
        val newsSourceBuilder = SearchSourceBuilder()
        newsSourceBuilder.query(searchAgentQueryBuilder)
        // retrieve the latest news
        newsSourceBuilder.sort(FieldSortBuilder("published_date").order(SortOrder.DESC))
        val updatedLastSearchedPublishedDate = Instant.now()

        newsSearchRequest.source(newsSourceBuilder)

        val newsResponse = elasticClient.search(newsSearchRequest, RequestOptions.DEFAULT)

        if (newsResponse.hits.hits.isNotEmpty()) {
            // deserialize the response
            val newsList: List<News> = newsResponse
                .hits.hits
                .map(this::createNews)

            emailService.sendEmail(EmailDetails(searchAgent, newsList))

        }

        searchAgentRepository.setSearchAgentLastPublishedDate(searchAgentId, updatedLastSearchedPublishedDate)
        searchAgentRepository.setLastSearchAgentHitProcessingWindow(searchAgentId, frequency, frequencyWindow)
    }

    fun createNews(searchHit: SearchHit): News {
        val source = searchHit.sourceAsString
        val sourceJson = JSONObject(source)
        val title = sourceJson.get("title") as String
        val body = sourceJson.get("body") as String
        val category = sourceJson.get("category") as String
        val publishedDate = Instant.parse(sourceJson.get("published_date") as String)
        return News(searchHit.id.toLong(), title, body, category, publishedDate)
    }


    fun createSearchAgent(searchAgentGetResponse: GetResponse): SearchAgent {
        val source = searchAgentGetResponse.sourceAsString
        val sourceJson = JSONObject(source)
        val name = sourceJson.get("name") as String
        val email = sourceJson.get("email") as String

        return SearchAgent(searchAgentGetResponse.id.toLong(), name, email)
    }

    fun stop() {
        logger.info("Stopping SearchAgentHitProcessorTask")
        stopping = true
        searchAgentHitConsumer.wakeup()
    }

    fun isStopping(): Boolean {
        return stopping
    }


    fun parseQuery(queryAsString: String): QueryBuilder {
        val parser: XContentParser = JsonXContent.jsonXContent.createParser(
            xContentRegistry,
            LoggingDeprecationHandler.INSTANCE, queryAsString
        )
        return AbstractQueryBuilder.parseInnerQueryBuilder(parser)
    }


    companion object {
        val CONSUMER_GROUP_ID = "search-agent-hit-processor"

        val xContentRegistry = NamedXContentRegistry(
            SearchModule(
                Settings.EMPTY,
                false,
                listOf()
            ).namedXContents
        )


        val NEWS_INDEX: String = "news"
        val NEWS_NOTIFY_INDEX: String = "news-notify"

    }
}