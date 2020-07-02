package com.findinpath.messenger.batch

data class EmailDetails(
    val searchAgent: SearchAgent,
    val newsList: List<News>
)