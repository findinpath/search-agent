package com.findinpath.percolator

import org.apache.http.HttpHost
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors


fun main(args: Array<String>) {

    val newsProcessor = NewsProcessor("localhost:9092", listOf(HttpHost("localhost", 9200, "http")), 1)
    try {
        newsProcessor.start()
    } catch (e: Throwable) {
        newsProcessor.stop()
    }
    newsProcessor.awaitStop()

}

class NewsProcessor(val kafkaBootstrapServers: String, val elasticHosts: List<HttpHost>, val tasks: Int) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val stopLatch = CountDownLatch(1)
    private val executorService = Executors.newCachedThreadPool()


    fun start() {
        (1..tasks).forEach {
            executorService.submit(NewsProcessorTask(kafkaBootstrapServers, elasticHosts))
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