package com.findinpath.messenger.batch

import java.time.Instant

interface SearchAgentRepository {

    /**
     * Returns the beginning of the time window (e.g : hour/ day) for which
     * a search agent hit for the search agent has been already processed.
     *
     * Use this method as a safe-guard to avoid sending duplicate messages for the
     * same processing window. During a time window (e.g.: hour/day) there could
     * be multiple matching hits for a search agent.
     * When dealing with a hit for a search agent, there will be checked whether
     * there was already a hit processed for the time window in question
     *
     * @param id the search agent identifier
     * @param frequency the frequency set for the search agent at the moment of finding the hit. NOTE that this
     * attribute may change over time (e.g. : from hour to day) for a given search agent.
     * @return the timestamp of the latest processing for the search agent in the frequency window specified
     * or null if nothing was set already for the search agent in the persistence
     */
    fun getLastSearchAgentHitProcessingWindow(id: Long, frequency: Frequency): Instant?

    /**
     * Update the timestamp of the latest processing for the search agent and frequency specified.
     *
     * @param id the search agent id
     * @param frequency the search agent frequency
     * @param windowBegin the timestamp corresponding to the beginning of the time window (e.g. : hour, day)
     */
    fun setLastSearchAgentHitProcessingWindow(id: Long, frequency: Frequency, windowBegin: Instant)


    /**
     * Returns the latest timestamp used by the search agent for filtering news index
     * after the publish date.
     *
     * @id the search agent id
     * @return the latest timestamp used by the search agent for filtering news or null
     * if nothing was set for the search agent in the persistence.
     */
    fun getSearchAgentLastPublishedDate(id: Long): Instant?


    /**
     * Update the timestamp used for filtering the news index after
     * the publish date.
     *
     * @param id the search agent id
     * @param instant the timestamp to be used for filtering news after the publish date.
     */
    fun setSearchAgentLastPublishedDate(id: Long, instant: Instant)
}