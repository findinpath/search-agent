package com.findinpath.percolator.scratches

import org.apache.http.HttpHost
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.percolator.PercolateQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.FieldSortBuilder
import org.elasticsearch.search.sort.SortOrder
import org.json.JSONObject


fun main(args: Array<String>) {
    val client = RestHighLevelClient(RestClient.builder(
        HttpHost("localhost", 9200, "http")
    ))

    //Build a document to check against the percolator
    val docBuilder = XContentFactory.jsonBuilder().startObject()
    docBuilder.field("category", "weather")
    docBuilder.field("title", "Snow on the ground, sun in the sky")
    docBuilder.field("body", "I am waiting for the day where kids can skate on the water and the dog can play in the snow while we are sitting in the sun.")
    docBuilder.endObject() //End of the JSON root object

    val percolateQuery = PercolateQueryBuilder("query", "_doc", BytesReference.bytes(docBuilder))





    client.use {restHighLevelClient ->

        var lastHitDocId: Int? = null
        do {
            val searchRequest = SearchRequest("news-notify")
            val sourceBuilder = SearchSourceBuilder()
            sourceBuilder.size(1)
            lastHitDocId?.let {
                sourceBuilder.searchAfter(arrayOf(it))
            }
            sourceBuilder.searchAfter()
            sourceBuilder.query(percolateQuery)
            sourceBuilder.sort(FieldSortBuilder("tie_breaker_id").order(SortOrder.ASC))
            searchRequest.source(sourceBuilder)


            val response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT)
            println(response)

            if (response.hits.hits.size > 0){
                val lastHit = response.hits.hits[response.hits.hits.size -1]
                lastHitDocId = JSONObject(lastHit.sourceAsString)["tie_breaker_id"] as Int
            }
        }while(response.hits.hits.size > 0)

    }





    client.close()
}