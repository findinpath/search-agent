package com.findinpath.percolator

import org.apache.http.HttpHost
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.percolator.PercolateQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.FieldSortBuilder
import org.elasticsearch.search.sort.SortOrder
import java.io.Closeable


class SearchAgentPercolator(hosts: List<HttpHost>) : Closeable{
    val client = RestHighLevelClient(RestClient.builder(*hosts.toTypedArray()))

    fun percolate(news : News, searchAfterSearchAgentId: Long? = null, pageSize: Int = PAGE_SIZE) : SearchResponse{


        val docBuilder = XContentFactory.jsonBuilder().startObject()
        docBuilder.field("category", news.category)
        docBuilder.field("title", news.title)
        docBuilder.field("body", news.body)
        docBuilder.endObject()

        val percolateQuery = PercolateQueryBuilder("query", BytesReference.bytes(docBuilder), XContentType.JSON)

        val searchRequest = SearchRequest("news-notify")
        val sourceBuilder = SearchSourceBuilder()
        sourceBuilder.size(pageSize)
        sourceBuilder.query(percolateQuery)
        searchRequest.source(sourceBuilder)


        searchAfterSearchAgentId?.let {
            sourceBuilder.searchAfter(arrayOf(it))
        }
        sourceBuilder.query(percolateQuery)
        sourceBuilder.sort(FieldSortBuilder("tie_breaker_id").order(SortOrder.ASC))
        searchRequest.source(sourceBuilder)


        return client.search(searchRequest, RequestOptions.DEFAULT)
    }


    override fun close() {
        client.close()
    }

    companion object{
        val PAGE_SIZE = 10
    }
}