package com.findinpath.messenger.immediate

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import java.lang.String
import java.time.Instant
import java.util.Date


class CassandraSearchAlertRepository(private val session: Session) : SearchAlertRepository {
    private val selectLastSearchAlertHitProcessingWindowPrepared = session.prepare(
        String.format(
            "SELECT * FROM %s.%s WHERE search_alert_id = ? and news_id = ?",
            KEYSPACE,
            SEARCH_ALERT_PROCESSED_NEWS_TABLE_NAME
        )
    )

    private val insertSearchAlertProcessedNewsPrepared: PreparedStatement = session.prepare(
        String.format(
            "INSERT INTO %s.%s (search_alert_id, news_id, processing_date) VALUES (?, ?, ?)",
            KEYSPACE,
            SEARCH_ALERT_PROCESSED_NEWS_TABLE_NAME
        )
    )


    override fun isProcessingDone(searchAlertId: Long, newsId: Long): Boolean {
        val selectBound = selectLastSearchAlertHitProcessingWindowPrepared.bind(searchAlertId, newsId)
        val resultSet: ResultSet = session.execute(selectBound)


        if (resultSet.iterator().hasNext()) {
            return true
        }
        return false
    }

    override fun markAsProcessed(searchAlertId: Long, newsId: Long, processingDate: Instant) {
        val insertBound = insertSearchAlertProcessedNewsPrepared.bind(searchAlertId, newsId, Date.from(processingDate))
        session.execute(insertBound)
    }

    companion object {
        const val KEYSPACE = "demo"
        const val SEARCH_ALERT_PROCESSED_NEWS_TABLE_NAME = "search_alert_processed_news"
    }
}