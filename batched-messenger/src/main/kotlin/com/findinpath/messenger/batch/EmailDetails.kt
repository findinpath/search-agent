package com.findinpath.messenger.batch

data class EmailDetails(
    val searchAlert: SearchAlert,
    val newsList: List<News>
)