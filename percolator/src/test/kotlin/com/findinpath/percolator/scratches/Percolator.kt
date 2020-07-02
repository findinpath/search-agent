package com.findinpath.percolator.scratches

import org.apache.http.HttpHost
import org.apache.http.HttpStatus
import org.apache.http.util.EntityUtils
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.percolator.PercolateQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
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


    // search an already indexed document
    //val percolateQuery = PercolateQueryBuilder("query", "news", null, "2", null, null, null)



    val searchRequest = SearchRequest("news-notify")
    val sourceBuilder = SearchSourceBuilder()
    sourceBuilder.query(percolateQuery)
    searchRequest.source(sourceBuilder)


    client.use {restHighLevelClient ->
        val response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT)
        println(response)


        for (hit in response.hits){

            val source = hit.sourceAsString
            val sourceJson = JSONObject(source)
            val queryJson = JSONObject().put("query",sourceJson.get("query")).toString()



            val newsRequest = Request("GET", "news/_search")
            newsRequest.setJsonEntity(queryJson)
            val newsResponse = restHighLevelClient.lowLevelClient.performRequest(newsRequest)

            if (newsResponse.statusLine.statusCode == HttpStatus.SC_OK){

                val newsResponseBody = EntityUtils.toString(newsResponse.entity)
                println(newsResponseBody)

                val newsResponseJsonHits = (JSONObject(newsResponseBody).get("hits") as JSONObject).get("hits")
                println(newsResponseJsonHits)
            }
        }


    }





    client.close()
}