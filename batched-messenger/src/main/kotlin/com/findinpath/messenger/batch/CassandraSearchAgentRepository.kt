package com.findinpath.messenger.batch

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import java.lang.String
import java.time.Instant
import java.util.Date


class CassandraSearchAgentRepository(private val session: Session) : SearchAgentRepository {
    private val selectLastSearchAgentHitProcessingWindowPrepared = session.prepare(
        String.format(
            "SELECT window FROM %s.%s WHERE id = ? and frequency = ?",
            KEYSPACE,
            SEARCH_AGENT_LAST_PROCESSING_WINDOW_TABLE_NAME
        )
    )

    private val insertLastSearchAgentHitProcessingWindowPrepared: PreparedStatement = session.prepare(
        String.format(
            "INSERT INTO %s.%s (id, frequency, window) VALUES (?, ?, ?)",
            KEYSPACE,
            SEARCH_AGENT_LAST_PROCESSING_WINDOW_TABLE_NAME
        )
    )


    private val selectSearchAgentLastPublishedDatePrepared = session.prepare(
        String.format(
            "SELECT last_published_date FROM %s.%s WHERE id = ? ",
            KEYSPACE,
            SEARCH_AGENT_LAST_PUBLISHED_DATE_TABLE_NAME
        )
    )

    private val insertSearchAgentLastPublishedDatePrepared: PreparedStatement = session.prepare(
        String.format(
            "INSERT INTO %s.%s (id, last_published_date) VALUES (?, ?)",
            KEYSPACE,
            SEARCH_AGENT_LAST_PUBLISHED_DATE_TABLE_NAME
        )
    )



    override fun getLastSearchAgentHitProcessingWindow(id: Long, frequency: Frequency): Instant? {
        val selectBound = selectLastSearchAgentHitProcessingWindowPrepared.bind(id, frequency.toString())
        val resultSet: ResultSet = session.execute(selectBound)


        if (resultSet.iterator().hasNext()) {
            return resultSet.one().getTimestamp("window").toInstant()
        }
        return null
    }

    override fun setLastSearchAgentHitProcessingWindow(id: Long, frequency: Frequency, windowBegin: Instant) {
        val insertBound = insertLastSearchAgentHitProcessingWindowPrepared.bind(id, frequency.toString(), Date.from(windowBegin))
        session.execute(insertBound)
    }

    override fun getSearchAgentLastPublishedDate(id: Long): Instant? {
        val selectBound = selectSearchAgentLastPublishedDatePrepared.bind(id)
        val resultSet: ResultSet = session.execute(selectBound)


        if (resultSet.iterator().hasNext()) {
            val lastPublishedDate = resultSet.one().getTimestamp("last_published_date")
            return lastPublishedDate.toInstant()
        }
        return null
    }

    override fun setSearchAgentLastPublishedDate(id: Long, instant: Instant) {
        val insertBound = insertSearchAgentLastPublishedDatePrepared.bind(id, Date.from(instant))
        session.execute(insertBound)
    }

    companion object {
        const val KEYSPACE = "demo"
        const val SEARCH_AGENT_LAST_PROCESSING_WINDOW_TABLE_NAME = "search_agent_last_processing_window"
        const val SEARCH_AGENT_LAST_PUBLISHED_DATE_TABLE_NAME = "search_agent_last_published_date"
    }
}