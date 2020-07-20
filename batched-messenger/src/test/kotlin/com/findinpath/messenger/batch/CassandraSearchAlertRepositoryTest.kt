package com.findinpath.messenger.batch

import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.CoreMatchers.nullValue
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Instant
import java.time.temporal.ChronoUnit


@Testcontainers
class CassandraSearchAlertRepositoryTest {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Test
    fun getLastSearchAlertHitProcessingWindowAccuracy(){
        cassandraContainer.cluster.connect().use {session->
            val repository = CassandraSearchAlertRepository(session)
            val searchAlertId = 1L
            var dbWindow = repository.getLastSearchAlertHitProcessingWindow(searchAlertId, Frequency.HOURLY)
            assertThat(dbWindow, `is`(nullValue()))

            val newWindow = Instant.now().truncatedTo(ChronoUnit.HOURS)
            repository.setLastSearchAlertHitProcessingWindow(searchAlertId, Frequency.HOURLY, newWindow)
            dbWindow = repository.getLastSearchAlertHitProcessingWindow(searchAlertId, Frequency.HOURLY)
            assertThat(dbWindow, equalTo(newWindow))
        }
    }


    @Test
    fun getSearchAlertLastPublishedDateAccuracy(){
        cassandraContainer.cluster.connect().use {session->
            val repository = CassandraSearchAlertRepository(session)
            val searchAlertId = 1L
            var dbLastPublishedDate = repository.getSearchAlertLastPublishedDate(searchAlertId)
            assertThat(dbLastPublishedDate, `is`(nullValue()))

            val newLastPublishedDate = Instant.now().truncatedTo(ChronoUnit.MILLIS)
            repository.setSearchAlertLastPublishedDate(searchAlertId, newLastPublishedDate)
            dbLastPublishedDate = repository.getSearchAlertLastPublishedDate(searchAlertId)
            assertThat(dbLastPublishedDate, equalTo(newLastPublishedDate))
        }
    }

    companion object{
        @Container
        val cassandraContainer = SpecifiedCassandraContainer("cassandra:3.11")

        private const val CREATE_DEMO_KEYSPACE_DDL = "CREATE KEYSPACE DEMO \n" +
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }"

        private const val CREATE_SEARCH_ALERT_LAST_PROCESSING_WINDOW_TABLE_DDL = "CREATE TABLE DEMO.SEARCH_ALERT_LAST_PROCESSING_WINDOW (\n" +
                "id BIGINT,\n" +
                "frequency VARCHAR,\n" +
                "window TIMESTAMP,\n" +
                "PRIMARY KEY (id, frequency)\n" +
                ")\n"

        private const val CREATE_SEARCH_ALERT_LAST_PUBLISHED_DATE_TABLE_DDL = "CREATE TABLE DEMO.SEARCH_ALERT_LAST_PUBLISHED_DATE (\n" +
                "id BIGINT,\n" +
                "last_published_date TIMESTAMP,\n" +
                "PRIMARY KEY (id)\n" +
                ")\n"

        @BeforeAll
        @JvmStatic
        internal fun setupCassandra(){
            cassandraContainer.cluster.connect().use { session ->
                session.execute(CREATE_DEMO_KEYSPACE_DDL)
                session.execute(CREATE_SEARCH_ALERT_LAST_PROCESSING_WINDOW_TABLE_DDL)
                session.execute(CREATE_SEARCH_ALERT_LAST_PUBLISHED_DATE_TABLE_DDL)

            }
        }
    }

    class SpecifiedCassandraContainer(image: String) : CassandraContainer<SpecifiedCassandraContainer>(image)
}