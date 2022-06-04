package com.mahama.elasticsearch.service;

import com.mahama.common.utils.StringUtil;
import com.mahama.elasticsearch.data.Article;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component("elasticsearch_SearchService")
@AllArgsConstructor
@Slf4j
public class SearchService {
    private final JestClient jestClient;

    public List<SearchResult.Hit<Article, Void>> search(String keyword, String indexName, boolean highlight) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.queryStringQuery(keyword));

        if (highlight) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            //path属性高亮度
            HighlightBuilder.Field highlightPath = new HighlightBuilder.Field("path");
            highlightPath.highlighterType("unified");
            highlightBuilder.field(highlightPath);
            //title字段高亮度
            HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field("title");
            highlightTitle.highlighterType("unified");
            highlightBuilder.field(highlightTitle);
            //content字段高亮度
            HighlightBuilder.Field highlightContent = new HighlightBuilder.Field("content");
            highlightContent.highlighterType("unified");
            highlightBuilder.field(highlightContent);

            //高亮度配置生效
            searchSourceBuilder.highlighter(highlightBuilder);
        }

//        log.info("搜索条件{}", searchSourceBuilder);

        //构建搜索功能
        var builder = new Search.Builder(searchSourceBuilder.toString());
        if (StringUtil.isNotNullOrEmpty(indexName)) {
            builder.addIndex(indexName);
        }
        builder.addType("files");
        Search search = builder.build();
        try {
            //执行
            SearchResult result = jestClient.execute(search);
            return result.getHits(Article.class);
        } catch (IOException e) {
            log.error("{}", e.getLocalizedMessage());
        }
        return null;
    }
}
