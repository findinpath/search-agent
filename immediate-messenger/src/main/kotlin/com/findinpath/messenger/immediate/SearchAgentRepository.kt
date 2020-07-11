package com.findinpath.messenger.immediate

import java.time.Instant

interface SearchAgentRepository {

    fun isProcessingDone(searchAgentId: Long, newsId: Long): Boolean

    fun markAsProcessed(searchAgentId: Long, newsId: Long, processingDate: Instant)

}