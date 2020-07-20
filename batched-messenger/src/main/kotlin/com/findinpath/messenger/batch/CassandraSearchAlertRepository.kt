package com.findinpath.messenger.batch

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import java.lang.String
import java.time.Instant
import java.util.Date


class CassandraSearchAlertRepository(private val session: Session) : SearchAlertRepository {
    private val selectLastSearchAlertHitProcessingWindowPrepared = session.prepare(
        String.format(
            "SELECT window FROM %s.%s WHERE id = ? and frequency = ?",
            KEYSPACE,
            SEARCH_ALERT_LAST_PROCESSING_WINDOW_TABLE_NAME
        )
    )

    private val insertLastSearchAlertHitProcessingWindowPrepared: PreparedStatement = session.prepare(
        String.format(
            "INSERT INTO %s.%s (id, frequency, window) VALUES (?, ?, ?)",
            KEYSPACE,
            SEARCH_ALERT_LAST_PROCESSING_WINDOW_TABLE_NAME
        )
    )


    private val selectSearchAlertLastPublishedDatePrepared = session.prepare(
        String.format(
            "SELECT last_published_date FROM %s.%s WHERE id = ? ",
            KEYSPACE,
            SEARCH_ALERT_LAST_PUBLISHED_DATE_TABLE_NAME
        )
    )

    private val insertSearchAlertLastPublishedDatePrepared: PreparedStatement = session.prepare(
        String.format(
            "INSERT INTO %s.%s (id, last_published_date) VALUES (?, ?)",
            KEYSPACE,
            SEARCH_ALERT_LAST_PUBLISHED_DATE_TABLE_NAME
        )
    )



    override fun getLastSearchAlertHitProcessingWindow(id: Long, frequency: Frequency): Instant? {
        val selectBound = selectLastSearchAlertHitProcessingWindowPrepared.bind(id, frequency.toString())
        val resultSet: ResultSet = session.execute(selectBound)


        if (resultSet.iterator().hasNext()) {
            return resultSet.one().getTimestamp("window").toInstant()
        }
        return null
    }

    override fun setLastSearchAlertHitProcessingWindow(id: Long, frequency: Frequency, windowBegin: Instant) {
        val insertBound = insertLastSearchAlertHitProcessingWindowPrepared.bind(id, frequency.toString(), Date.from(windowBegin))
        session.execute(insertBound)
    }

    override fun getSearchAlertLastPublishedDate(id: Long): Instant? {
        val selectBound = selectSearchAlertLastPublishedDatePrepared.bind(id)
        val resultSet: ResultSet = session.execute(selectBound)


        if (resultSet.iterator().hasNext()) {
            val lastPublishedDate = resultSet.one().getTimestamp("last_published_date")
            return lastPublishedDate.toInstant()
        }
        return null
    }

    override fun setSearchAlertLastPublishedDate(id: Long, instant: Instant) {
        val insertBound = insertSearchAlertLastPublishedDatePrepared.bind(id, Date.from(instant))
        session.execute(insertBound)
    }

    companion object {
        const val KEYSPACE = "demo"
        const val SEARCH_ALERT_LAST_PROCESSING_WINDOW_TABLE_NAME = "search_alert_last_processing_window"
        const val SEARCH_ALERT_LAST_PUBLISHED_DATE_TABLE_NAME = "search_alert_last_published_date"
    }
}