package com.findinpath.percolator

import org.apache.http.HttpHost
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Properties

class NewsProcessorTask(kafkaBootstrapServers: String,
                        elasticHosts: List<HttpHost>,
                        val newsConsumerGroupId: String = CONSUMER_GROUP_ID,
                        val percolationPageSize: Int = SearchAgentPercolator.PAGE_SIZE) : Runnable {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val newsConsumer = createConsumer(kafkaBootstrapServers)
    private val searchAgentHitProducer = createProducer(kafkaBootstrapServers)
    private val searchAgentPercolator = SearchAgentPercolator(elasticHosts)
    private var stopping: Boolean = false
    var partitionsAssigned: Boolean = false
        get() = field


    private fun createConsumer(brokers: String): Consumer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["group.id"] = newsConsumerGroupId
        props["key.deserializer"] = StringDeserializer::class.java
        props["value.deserializer"] = StringDeserializer::class.java
        props["enable.auto.commit"] = "false"
        return KafkaConsumer<String, String>(props)
    }

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = StringSerializer::class.java
        return KafkaProducer<String, String>(props)
    }


    override fun run() {
        newsConsumer.subscribe(listOf(newsTopic) , object: ConsumerRebalanceListener{
            override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {
                logger.info("Partitions ${partitions} were assigned on the news processor task")
                partitionsAssigned = true
            }

            override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
            }
        })

        logger.info("Consuming news for notifying search agents")

        while (!stopping) {
            val records = newsConsumer.poll(Duration.ofSeconds(1))
            logger.info("Received ${records.count()} news records")

            records.iterator().forEach {
                val newsJson = it.value()
                var news:News? = null
                try {
                    news = jsonMapper.readValue(newsJson, News::class.java)
                }catch(e:Exception){
                    logger.error("Exception occurred while deserializing JSON content $newsJson", e)

                }
                if (news != null) {
                    try {
                        process(news)
                    }catch(e:Exception){
                        logger.error("Exception occurred while processing news $news", e)
                    }
                }
            }

            try {
                newsConsumer.commitSync();
            } catch (e: CommitFailedException) {
                logger.error("Commit of the Apache Kafka offset failed", e)
            }
        }
    }



    fun stop() {
        logger.info("Stopping NewsProcessorTask")
        stopping = true
        newsConsumer.wakeup()
        searchAgentHitProducer.close(Duration.ofMillis(100))
    }

    fun process(news: News) {
        var lastHitDocId: Long? = null
        do {
            val response = searchAgentPercolator.percolate(news, lastHitDocId, percolationPageSize)

            if (response.hits.hits.isNotEmpty()) {
                logger.info("Retrieved ${response.hits.hits.size} hits from the search agent percolator for the news ${news.id}")
                response.hits.hits.forEach { hit ->
                    val hitJson = JSONObject(hit.sourceAsString)

                    val searchAgentId = hit.id.toLong()
                    val query = hitJson.get("query").toString()
                    val email = hitJson.get("email").toString()
                    val frequency = Frequency.valueOf(hitJson.get("frequency").toString())

                    val now = Instant.now()
                    val searchAgentHit = SearchAgentHit(news.id, searchAgentId, email, frequency, query, now)

                    // produce stuff
                    // write to Kafka the search agent info depending on the search agent type : immediate/hourly/daily
                    val topic = when (frequency) {
                        Frequency.IMMEDIATE -> immediateTopic
                        Frequency.HOURLY -> getNextHourTopic(now)
                        Frequency.DAILY -> getNextDayTopic(now)
                    }
                    logger.info("Sending to $topic the hit with ID ${hit.id}")
                    searchAgentHitProducer
                        .send(ProducerRecord(topic, hit.id, jsonMapper.writeValueAsString(searchAgentHit)))
                        .get() // wait for the write acknowledgement
                }
            }

            if (response.hits.hits.size == SearchAgentPercolator.PAGE_SIZE) {
                // get the next page

                val lastHit = response.hits.hits[response.hits.hits.size - 1]
                lastHitDocId = JSONObject(lastHit.sourceAsString)["tie_breaker_id"] as Long
            }

            //TODO maybe save record partition, offset, lasthitdoc id in Cassandra
            // in order to avoid sending duplicates (e.g.: once per percolated batch)

        } while (response.hits.hits.size == SearchAgentPercolator.PAGE_SIZE)
    }


    private fun getNextHourTopic(instant: Instant): String {
        return "hour-" + instant.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS).toEpochMilli()
    }

    private fun getNextDayTopic(instant: Instant): String {
        return "day-" + instant.truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.HOURS).toEpochMilli()
    }

    companion object{
        val CONSUMER_GROUP_ID = "news-processor"
    }
}
