package com.findinpath.messenger.immediate

import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class ImmediateSearchAlertHitProcessor(val kafkaBootstrapServers: String,
                                       val emailService: EmailService,
                                       val searchAlertRepository: SearchAlertRepository,
                                       val tasks: Int,
                                       val topic: String) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val stopLatch = CountDownLatch(1)
    private val executorService = Executors.newCachedThreadPool()


    fun start() {
        (1..tasks).forEach {
            executorService.submit(
                ImmediateSearchAlertHitProcessorTask(
                    kafkaBootstrapServers,
                    emailService,
                    searchAlertRepository,
                    topic
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