package com.findinpath.percolator.scratches

import org.apache.http.HttpHost
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.index.query.AbstractQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.percolator.PercolateQueryBuilder
import org.elasticsearch.search.SearchModule
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


    val searchRequest = SearchRequest("news-notify")
    val sourceBuilder = SearchSourceBuilder()
    sourceBuilder.query(percolateQuery)
    searchRequest.source(sourceBuilder)


    val xContentRegistry = xContentRegistry()

    client.use {restHighLevelClient ->
        val response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT)
        println(response)


        for (hit in response.hits){

            val source = hit.sourceAsString
            val sourceJson = JSONObject(source)
            val queryAsString = sourceJson.get("query").toString()
            println(queryAsString)
            val newsQueryBuilder = parseQuery(
                xContentRegistry,
                sourceJson.get("query").toString()
            )

            //(newsQueryBuilder as BoolQueryBuilder).filter(QueryBuilders.rangeQuery("publishDate").gt(...))

            val newsSearchRequest = SearchRequest("news")
            val newsSourceBuilder = SearchSourceBuilder()
            newsSourceBuilder.query(newsQueryBuilder)
            newsSearchRequest.source(newsSourceBuilder)


            val newsResponse = restHighLevelClient.search(newsSearchRequest, RequestOptions.DEFAULT)
            println(newsResponse)
        }
    }

    client.close()
}

fun parseQuery(xContentRegistry: NamedXContentRegistry, queryAsString: String): QueryBuilder {
    val parser: XContentParser = JsonXContent.jsonXContent.createParser(xContentRegistry,LoggingDeprecationHandler.INSTANCE, queryAsString)
    return AbstractQueryBuilder.parseInnerQueryBuilder(parser)
}

fun xContentRegistry(): NamedXContentRegistry {
    return NamedXContentRegistry(
        SearchModule(
            Settings.EMPTY,
            false,
            listOf()
        ).namedXContents
    )
}
