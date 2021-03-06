package com.findinpath.messenger.immediate

import java.time.Instant

data class News(
        val id: Long,
        val title: String,
        val body: String,
        val category: String,
        val publishedDate: Instant
)