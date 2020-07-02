package com.findinpath.messenger.immediate

import java.time.Instant

data class ImmediateSearchAgentHit(
    val searchAgent: SearchAgent,
    val news: News
)