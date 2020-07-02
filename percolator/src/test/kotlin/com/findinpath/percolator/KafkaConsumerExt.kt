package com.findinpath.percolator

import org.apache.kafka.clients.consumer.Consumer
import java.time.Duration

private const val POLL_INTERVAL = 100L
private const val POLL_TIMEOUT = 20_000L

fun <K, V> Consumer<K, V>.waitFor(topics: List<String>, minCount: Int) = sequence {
    subscribe(topics)
    var counter = 0
    var start = System.currentTimeMillis()
    while (true) {
        val records = poll(Duration.ofMillis(POLL_INTERVAL))
        records.forEach {
            counter++
            yield(it)
        }
        if (counter >= minCount) {
            break
        }
        if (System.currentTimeMillis() - start > POLL_TIMEOUT) {
            throw IllegalStateException("Timed out waiting for $minCount from $topics, only received $counter")
        }
    }
}