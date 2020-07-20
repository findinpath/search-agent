package com.findinpath.messenger.batch

import java.time.Instant

interface SearchAlertRepository {

    /**
     * Returns the beginning of the time window (e.g : hour/ day) for which
     * a search alert hit for the search alert has been already processed.
     *
     * Use this method as a safe-guard to avoid sending duplicate messages for the
     * same processing window. During a time window (e.g.: hour/day) there could
     * be multiple matching hits for a search alert.
     * When dealing with a hit for a search alert, there will be checked whether
     * there was already a hit processed for the time window in question
     *
     * @param id the search alert identifier
     * @param frequency the frequency set for the search alert at the moment of finding the hit. NOTE that this
     * attribute may change over time (e.g. : from hour to day) for a given search alert.
     * @return the timestamp of the latest processing for the search alert in the frequency window specified
     * or null if nothing was set already for the search alert in the persistence
     */
    fun getLastSearchAlertHitProcessingWindow(id: Long, frequency: Frequency): Instant?

    /**
     * Update the timestamp of the latest processing for the search alert and frequency specified.
     *
     * @param id the search alert id
     * @param frequency the search alert frequency
     * @param windowBegin the timestamp corresponding to the beginning of the time window (e.g. : hour, day)
     */
    fun setLastSearchAlertHitProcessingWindow(id: Long, frequency: Frequency, windowBegin: Instant)


    /**
     * Returns the latest timestamp used by the search alert for filtering news index
     * after the publish date.
     *
     * @id the search alert id
     * @return the latest timestamp used by the search alert for filtering news or null
     * if nothing was set for the search alert in the persistence.
     */
    fun getSearchAlertLastPublishedDate(id: Long): Instant?


    /**
     * Update the timestamp used for filtering the news index after
     * the publish date.
     *
     * @param id the search alert id
     * @param instant the timestamp to be used for filtering news after the publish date.
     */
    fun setSearchAlertLastPublishedDate(id: Long, instant: Instant)
}