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
import java.time.Instant
import java.util.Properties

class ImmediateSearchAlertHitProcessorTask(kafkaBootstrapServers: String,
                                           private val emailService: EmailService,
                                           private val searchAlertRepository: SearchAlertRepository,
                                           val topic: String,
                                           searchAlertHitConsumerGroupId: String = CONSUMER_GROUP_ID
): Runnable{

    private val logger = LoggerFactory.getLogger(javaClass)

    private val searchAlertHitConsumer = createConsumer(kafkaBootstrapServers, searchAlertHitConsumerGroupId)

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
        searchAlertHitConsumer.subscribe(listOf(topic) , object: ConsumerRebalanceListener {
            override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {
                logger.info("Partitions ${partitions} for the topic ${topic} were assigned on the search alert hit task")
                partitionsAssigned = true
            }

            override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
            }
        })


        while (!stopping) {
            val records = searchAlertHitConsumer.poll(Duration.ofSeconds(1))
            logger.info("Received ${records.count()} search alert hit records")

            records.iterator().forEach {
                val searchAlertHitJson = it.value()
                var searchAlertHit: ImmediateSearchAlertHit? = null
                try {
                    searchAlertHit = jsonMapper.readValue(searchAlertHitJson, ImmediateSearchAlertHit::class.java)
                }catch(e:Exception){
                    logger.error("Exception occurred while deserializing JSON content $searchAlertHitJson", e)

                }
                if (searchAlertHit != null) {
                    try {
                        process(searchAlertHit)
                    }catch(e:Exception){
                        logger.error("Exception occurred while processing search alert hit $searchAlertHit", e)
                    }
                }
            }

            try {
                searchAlertHitConsumer.commitSync();
            } catch (e: CommitFailedException) {
                logger.error("Commit of the Apache Kafka offset failed", e)
            }
        }

    }

    fun process(searchAlertHit: ImmediateSearchAlertHit){
        if (searchAlertRepository.isProcessingDone(searchAlertHit.searchAlert.id, searchAlertHit.news.id)){
            // avoid sending duplicate messages (in case of failures on the percolator side)
            return
        }

        emailService.sendEmail(EmailDetails(searchAlertHit.searchAlert, searchAlertHit.news))

        searchAlertRepository.markAsProcessed(searchAlertHit.searchAlert.id, searchAlertHit.news.id, Instant.now())
    }

    fun stop() {
        logger.info("Stopping ImmediateSearchAlertHitProcessorTask")
        stopping = true
        searchAlertHitConsumer.wakeup()
    }


    companion object{
        val CONSUMER_GROUP_ID = "search-alert-hit-processor"
    }
}