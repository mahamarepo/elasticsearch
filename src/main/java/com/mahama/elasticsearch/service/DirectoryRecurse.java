package com.mahama.elasticsearch.service;

import com.alibaba.fastjson.JSONObject;
import com.mahama.common.utils.MD5;
import com.mahama.common.utils.StringUtil;
import com.mahama.elasticsearch.data.Article;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pdfbox.io.RandomAccessFile;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;

@Component("elasticsearch_DirectoryRecurse")
@Slf4j
@AllArgsConstructor
public class DirectoryRecurse {
    private final JestClient jestClient;
    private final String typeName = "files";

    //读取文件内容转换为字符串
    private String readToString(File file, String fileType) {
        StringBuilder result = new StringBuilder();
        switch (fileType) {
            case "text/plain":
            case "java":
            case "c":
            case "cpp":
            case "txt":
                try (FileInputStream in = new FileInputStream(file)) {
                    long fileLength = file.length();
                    byte[] fileContent = new byte[(int) fileLength];
                    in.read(fileContent);
                    result.append(new String(fileContent, StandardCharsets.UTF_8));
                } catch (IOException e) {
                    log.error("{}", e.getLocalizedMessage());
                }
                break;
//            case "doc":
//                //使用HWPF组件中WordExtractor类从Word文档中提取文本或段落
//                try (FileInputStream in = new FileInputStream(file)) {
//                    WordExtractor extractor = new WordExtractor(in);
//                    result.append(extractor.getText());
//                } catch (Exception e) {
//                    log.error("{}", e.getLocalizedMessage());
//                }
//                break;
            case "docx":
                try (FileInputStream in = new FileInputStream(file); XWPFDocument doc = new XWPFDocument(in)) {
                    XWPFWordExtractor extractor = new XWPFWordExtractor(doc);
                    result.append(extractor.getText());
                } catch (Exception e) {
                    log.error("{}", e.getLocalizedMessage());
                }
                break;
            case "pdf":
                PDFParser parser;
                PDDocument pdfdocument = null;
                try {
                    parser = new PDFParser(new RandomAccessFile(file, "r"));
                    // 对PDF文件进行解析
                    parser.parse();
                    // 获取解析后得到的PDF文档对象
                    pdfdocument = parser.getPDDocument();
                    PDFTextStripper stripper = new PDFTextStripper();
                    result.append(stripper.getText(pdfdocument));
                } catch (IOException ignored) {
                } finally {
                    if (pdfdocument != null) {
                        try {
                            pdfdocument.close();
                        } catch (IOException ignored) {
                        }
                    }
                }
                break;
        }
        return result.toString();
    }

    //判断是否已经索引
    private JSONObject isIndex(File file, String indexName) {
        JSONObject result = new JSONObject();
        //用MD5生成文件指纹,搜索该指纹是否已经索引
        String fileFingerprint = MD5.getFileMD5String(file);
        result.put("fileFingerprint", fileFingerprint);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("fileFingerprint.keyword", fileFingerprint));
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType(typeName).build();
        try {
            //执行
            SearchResult searchResult = jestClient.execute(search);
            if (searchResult.getTotal() > 0) {
                result.put("isIndex", true);
            } else {
                result.put("isIndex", false);
            }
        } catch (Exception e) {
            result.put("isIndex", false);
        }
        return result;
    }

    //对文件目录及内容创建索引
    private void createIndex(File file, String indexName) {
        //忽略掉临时文件，以~$起始的文件名
        if (file.getName().startsWith("~$")) return;

        String fileType;
        String filename = file.getName();
        String[] strArray = filename.split("\\.");
        int suffixIndex = strArray.length - 1;
        fileType = strArray[suffixIndex];

        switch (fileType) {
            case "text/plain":
            case "java":
            case "c":
            case "cpp":
            case "txt":
//            case "doc":
            case "docx":
            case "pdf":
                JSONObject isIndexResult = isIndex(file, indexName);
                if (isIndexResult.getBoolean("isIndex")) break;
                log.info("构建索引----文件名：{}，文件类型：{}，MD5：{}", file.getPath(), fileType, isIndexResult.getString("fileFingerprint"));
                //1\. 给ES中索引(保存)一个文档
                Article article = new Article();
                article.setTitle(file.getName());
                article.setAuthor(file.getParent());
                article.setPath(file.getPath());
                article.setContent(readToString(file, fileType));
                article.setFileFingerprint(isIndexResult.getString("fileFingerprint"));
                //2\. 构建一个索引
                Index index = new Index.Builder(article).index(indexName).type(typeName).build();
                try {
                    //3\. 执行
                    if (StringUtil.isNotNullOrEmpty(jestClient.execute(index).getId())) {
                        log.info("构建索引成功！");
                    } else {
                        log.info("构建索引失败！");
                    }
                } catch (IOException e) {
                    log.error("{}", e.getLocalizedMessage());
                }
                break;
        }
    }

    public void find(String pathName, String indexName) throws IOException {
        //获取pathName的File对象
        File dirFile = new File(pathName);

        //判断该文件或目录是否存在，不存在时在控制台输出提醒
        if (!dirFile.exists()) {
//            log.info("文件或目录是不存在");
            return;
        }

        //判断如果不是一个目录，就判断是不是一个文件，时文件则输出文件路径
        if (!dirFile.isDirectory()) {
            if (dirFile.isFile()) {
                createIndex(dirFile, indexName);
            }
            return;
        }

        //获取此目录下的所有文件名与目录名
        String[] fileList = dirFile.list();

        for (String string : fileList) {
            //遍历文件目录
            File file = new File(dirFile.getPath(), string);
            //如果是一个目录，输出目录名后，进行递归
            if (file.isDirectory()) {
                //递归
                find(file.getCanonicalPath(), indexName);
            } else {
                createIndex(file, indexName);
            }
        }
    }
}
