package com.findinpath.messenger.immediate

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import java.lang.String
import java.time.Instant
import java.util.Date


class CassandraSearchAgentRepository(private val session: Session) : SearchAgentRepository {
    private val selectLastSearchAgentHitProcessingWindowPrepared = session.prepare(
        String.format(
            "SELECT * FROM %s.%s WHERE search_agent_id = ? and news_id = ?",
            KEYSPACE,
            SEARCH_AGENT_PROCESSED_NEWS_TABLE_NAME
        )
    )

    private val insertSearchAgentProcessedNewsPrepared: PreparedStatement = session.prepare(
        String.format(
            "INSERT INTO %s.%s (search_agent_id, news_id, processing_date) VALUES (?, ?, ?)",
            KEYSPACE,
            SEARCH_AGENT_PROCESSED_NEWS_TABLE_NAME
        )
    )


    override fun isProcessingDone(searchAgentId: Long, newsId: Long): Boolean {
        val selectBound = selectLastSearchAgentHitProcessingWindowPrepared.bind(searchAgentId, newsId)
        val resultSet: ResultSet = session.execute(selectBound)


        if (resultSet.iterator().hasNext()) {
            return true
        }
        return false
    }

    override fun markAsProcessed(searchAgentId: Long, newsId: Long, processingDate: Instant) {
        val insertBound = insertSearchAgentProcessedNewsPrepared.bind(searchAgentId, newsId, Date.from(processingDate))
        session.execute(insertBound)
    }

    companion object {
        const val KEYSPACE = "demo"
        const val SEARCH_AGENT_PROCESSED_NEWS_TABLE_NAME = "search_agent_processed_news"
    }
}