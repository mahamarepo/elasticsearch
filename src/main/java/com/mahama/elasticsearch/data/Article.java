package com.mahama.elasticsearch.data;

import io.searchbox.annotations.JestId;
import lombok.Data;

@Data
public class Article {
    @JestId
    private String id;
    private String author;
    private String title;
    private String path;
    private String content;
    private String fileFingerprint;
}
