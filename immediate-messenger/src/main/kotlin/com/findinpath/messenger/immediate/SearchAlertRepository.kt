package com.findinpath.messenger.immediate

import java.time.Instant

interface SearchAlertRepository {

    fun isProcessingDone(searchAlertId: Long, newsId: Long): Boolean

    fun markAsProcessed(searchAlertId: Long, newsId: Long, processingDate: Instant)

}