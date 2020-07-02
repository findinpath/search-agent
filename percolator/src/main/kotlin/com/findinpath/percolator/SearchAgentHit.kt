package com.findinpath.percolator

import java.time.Instant

data class SearchAgentHit(
    val newsId: Long,
    val searchAgentId: Long,
    val searchAgentEmail: String,
    val searchAgentFrequency: Frequency,
    val searchAgentQuery: String,
    val instant: Instant
)