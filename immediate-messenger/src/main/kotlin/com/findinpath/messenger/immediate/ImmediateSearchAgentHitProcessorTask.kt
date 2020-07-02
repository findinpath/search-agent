package com.findinpath.messenger.immediate

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties

class ImmediateSearchAgentHitProcessorTask(kafkaBootstrapServers: String,
                                           private val emailService: EmailService,
                                           val topic: String,
                                           searchAgentHitConsumerGroupId: String = CONSUMER_GROUP_ID
): Runnable{

    private val logger = LoggerFactory.getLogger(javaClass)

    private val searchAgentHitConsumer = createConsumer(kafkaBootstrapServers, searchAgentHitConsumerGroupId)

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
        searchAgentHitConsumer.subscribe(listOf(topic) , object: ConsumerRebalanceListener {
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

            records.iterator().forEach {
                val searchAgentHitJson = it.value()
                var searchAgentHit: ImmediateSearchAgentHit? = null
                try {
                    searchAgentHit = jsonMapper.readValue(searchAgentHitJson, ImmediateSearchAgentHit::class.java)
                }catch(e:Exception){
                    logger.error("Exception occurred while deserializing JSON content $searchAgentHitJson", e)

                }
                if (searchAgentHit != null) {
                    try {
                        process(searchAgentHit)
                    }catch(e:Exception){
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

    fun process(searchAgentHit: ImmediateSearchAgentHit){
        //TODO avoid duplicate emails by checking into Cassandra
        // < search agent id, news id, news published date>

        emailService.sendEmail(EmailDetails(searchAgentHit.searchAgent, searchAgentHit.news))
    }

    fun stop() {
        logger.info("Stopping ImmediateSearchAgentHitProcessorTask")
        stopping = true
        searchAgentHitConsumer.wakeup()
    }


    companion object{
        val CONSUMER_GROUP_ID = "search-agent-hit-processor"
    }
}