package com.findinpath.messenger.batch

import java.time.Instant

data class BatchedSearchAgentHit(
    val searchAgentId: Long,
    val newsId: Long
)