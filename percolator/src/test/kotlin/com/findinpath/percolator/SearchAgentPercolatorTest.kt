package com.findinpath.percolator

import org.apache.http.HttpHost
import org.elasticsearch.Version
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.*
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.security.RefreshPolicy
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.QueryBuilders.termQuery
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.json.JSONObject
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.io.IOException
import java.time.Instant


@Testcontainers
class SearchAgentPercolatorTest {

    @BeforeEach
    fun setup() {
        getClient(elasticsearchContainer).use { client ->
            createNewsIndex(client)
            createNewsNotifyIndex(client)
        }
    }

    @Test
    fun percolationDemo() {
        getClient(elasticsearchContainer).use { client ->

            val news = News(
                1L,
                "Early snow this year",
                "After a year with hardly any snow, this is going to be a serious winter",
                "weather",
                Instant.now()
            )


            indexNewsNotifyDocument(client, 1, "contact@mail.com", Frequency.IMMEDIATE, "snow", "weather")

            val searchAgentPercolator = SearchAgentPercolator(listOf(HttpHost.create(elasticsearchContainer.httpHostAddress)))
            searchAgentPercolator.use {
                val response = it.percolate(news)
                logger.info(response.toString())

                assertThat(response.hits.totalHits.value, equalTo(1L))
            }

        }
    }


    @Test
    fun percolationPagingDemo() {
        val news = News(
            1L,
            "Early snow this year",
            "After a year with hardly any snow, this is going to be a serious winter",
            "weather",
            Instant.now()
        )

        getClient(elasticsearchContainer).use { client ->



            val searchAgentsCount = 20
            (1..searchAgentsCount).forEach {
                indexNewsNotifyDocument(client, it.toLong(), "contact@mail.com", Frequency.HOURLY, "snow", "weather")
            }

            val searchAgentPercolator = SearchAgentPercolator(listOf(HttpHost.create(elasticsearchContainer.httpHostAddress)))
            val pageSize = 3
            val searchAgentIds = ArrayList<Long>()
            searchAgentPercolator.use {
                var lastHitDocId: Long? = null

                do {
                    val response = searchAgentPercolator.percolate(news, lastHitDocId, pageSize)

                    if (response.hits.hits.size > 0) {
                        searchAgentIds.addAll(response.hits.hits.map { it.id.toLong() })
                        logger.info(response.toString())
                    }

                    if (response.hits.hits.size == pageSize) {
                        // get the next page

                        val lastHit = response.hits.hits[response.hits.hits.size - 1]
                        lastHitDocId = (JSONObject(lastHit.sourceAsString)["tie_breaker_id"] as Int).toLong()
                    }

                } while (response.hits.hits.size == pageSize)
            }

            assertThat(searchAgentIds, equalTo((1..searchAgentsCount.toLong()).toList()))

        }
    }


    private fun createNewsIndex(client: RestHighLevelClient) {
        if (client.indices().exists(GetIndexRequest(NEWS_INDEX), RequestOptions.DEFAULT)) {
            client.indices().delete(DeleteIndexRequest(NEWS_INDEX), RequestOptions.DEFAULT)
        }

        val createNewsIndexRequest = CreateIndexRequest(NEWS_INDEX)
        val newsMappingsbuilder = jsonBuilder()
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
            .filter(termQuery("category", category))


        val sourcebuilder = jsonBuilder()
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

    private fun getClient(container: ElasticsearchContainer): RestHighLevelClient {
        return RestHighLevelClient(RestClient.builder(HttpHost.create(container.httpHostAddress)))
    }

    companion object {
        /**
         * Elasticsearch version which should be used for the Tests
         */
        val ELASTICSEARCH_VERSION: String = Version.CURRENT.toString()

        val NEWS_INDEX: String = "news"
        val NEWS_NOTIFY_INDEX: String = "news-notify"

        private val logger = LoggerFactory.getLogger(javaClass)

        @Container
        val elasticsearchContainer =
            ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:$ELASTICSEARCH_VERSION")
    }

}