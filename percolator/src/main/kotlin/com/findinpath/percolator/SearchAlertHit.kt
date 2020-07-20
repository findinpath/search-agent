package com.findinpath.percolator

import java.time.Instant

data class SearchAlertHit(
    val newsId: Long,
    val searchAlertId: Long,
    val searchAlertEmail: String,
    val searchAlertFrequency: Frequency,
    val searchAlertQuery: String,
    val instant: Instant
)