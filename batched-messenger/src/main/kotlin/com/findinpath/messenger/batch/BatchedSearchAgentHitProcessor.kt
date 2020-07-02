package com.findinpath.messenger.batch

import org.apache.http.HttpHost
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class BatchedSearchAgentHitProcessor(val kafkaBootstrapServers: String,
                                     val elasticHosts: List<HttpHost>,
                                     val searchAgentRepository: SearchAgentRepository,
                                     val emailService: EmailService,
                                     val tasks: Int,
                                     val topic: String,
                                     val frequency: Frequency,
                                     val frequencyWindow: Instant) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val stopLatch = CountDownLatch(1)
    private val executorService = Executors.newCachedThreadPool()


    fun start() {
        (1..tasks).forEach {
            executorService.submit(
                BatchedSearchAgentHitProcessorTask(
                    kafkaBootstrapServers,
                    elasticHosts,
                    searchAgentRepository,
                    emailService,
                    topic,
                    frequency,
                    frequencyWindow
                )
            )
        }
    }

    fun stop() {
        try {

        } finally {
            stopLatch.countDown()
        }
    }

    fun awaitStop() {
        try {
            stopLatch.await()
        } catch (e: InterruptedException) {
            logger.error("Interrupted waiting for Kafka Connect to shutdown")
        }
    }

}