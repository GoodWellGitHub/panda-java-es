package com.org.java.es.util;

import io.netty.util.internal.StringUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchTool {
    GetRequest request;
    String index;
    String documentId;

    private static String esIP = "localhost";
    private static int esPORT = 9200;
    private static int esPORT1 = 9201;
    private static String HTTP = "http";
    private static String INDEX = "es_index";
    private static RestHighLevelClient client = null;

    public static void init() {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));//初始化
    }

    public static void close() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void buildIndex(String index, String document, Map<String, Object> content) {
        IndexRequest indexRequest = new IndexRequest(index);
        if (!StringUtil.isNullOrEmpty(documentId)) {
            indexRequest.id(documentId);
        }
        indexRequest.source(content);
        IndexResponse indexResponse= null;
        try {
            indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(indexResponse);
    }

    public ElasticsearchTool(String index, String documentId) {
        this.index = index;
        this.documentId = documentId;
        this.request = new GetRequest(index, documentId);
        request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);//Disable source retrieval, enabled by default

        String[] includes = new String[]{"message", "*Date"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);//Configure source inclusion for specific fields
        request.routing("routing");
        request.preference("preference");
        request.realtime(false);
        request.refresh(true);
        request.version(2);
        request.versionType(VersionType.EXTERNAL);
    }

    /**
     * 同步执行
     * 以下列方式执行GetRequest时，客户端在继续执行代码之前等待返回GetResponse;
     * 如果无法解析高级REST客户端中的REST响应，请求超时或类似情况没有从服务器返回响应，则同步调用可能会抛出IOException。
     * 如果服务器返回4xx或5xx错误代码，则高级客户端会尝试解析响应正文错误详细信息，
     * 然后抛出通用ElasticsearchException并将原始ResponseException作为抑制异常添加到其中。
     */
    public void synchronousExecution(String index,String documentId) {
        try {
            GetRequest request=new GetRequest(index,documentId);
            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            System.out.println(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 异步执行
     * 执行GetRequest也可以以异步方式完成，以便客户端可以直接返回。
     * 用户需要通过将请求和侦听器传递给异步get方法来指定响应或潜在故障的处理方式。
     * 异步方法不会阻塞并立即返回。 一旦完成，如果执行成功完成，则使用onResponse方法回调ActionListener，
     * 如果失败则使用onFailure方法。 故障情形和预期异常与同步执行情况相同。
     */
    public void asynchronousExecution() {
        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            public void onResponse(GetResponse documentFields) {
                handleResponse(documentFields);
            }

            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        client.getAsync(request, RequestOptions.DEFAULT, listener);
    }

    public void handleResponse(GetResponse response) {
        String index = response.getIndex();
        String id = response.getId();
        if (response.isExists()) {
            long version = response.getVersion();
            String sourceAsString = response.getSourceAsString();
            Map<String, Object> sourceAsMap = response.getSourceAsMap();
            System.out.println(sourceAsMap);
            byte[] sourceAsBytes = response.getSourceAsBytes();
        } else {
            System.out.println("this document is not exist !");
        }
    }

    public void delete(String index, String documentId) {
        DeleteRequest request = new DeleteRequest(index, documentId);
        try {
            client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void update(String index, String documentId) {
        UpdateRequest request = new UpdateRequest(index, documentId);
        Map<String, Object> parameter = new HashMap<String, Object>();
        parameter.put("url", "www.baidu.com");
        //Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.field += params.count", parameter);
        //request.script(inline);

        Script stored = new Script(ScriptType.STORED, null, "increment-field", parameter);
        request.script(stored);

        request.routing("routing");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        request.retryOnConflict(3);
        request.fetchSource(true);
        request.setIfSeqNo(2L);
        request.setIfPrimaryTerm(1L);
        request.detectNoop(false);
        request.scriptedUpsert(true);
        request.docAsUpsert(true);
        request.waitForActiveShards(2);
        request.waitForActiveShards(ActiveShardCount.ALL);


        UpdateResponse response = null;
        try {
            response = client.update(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(response);
    }

    public void termQuery(String index,String filed,String fieldValue) {
        TermVectorsRequest request=new TermVectorsRequest(index,fieldValue);
        request.setFields(filed);

        request.setFieldStatistics(false);
        request.setTermStatistics(true);
        request.setPositions(false);
        request.setOffsets(false);
        request.setPayloads(false);

        Map<String, Integer> filterSettings = new HashMap<String,Integer>();
        filterSettings.put("max_num_terms", 3);
        filterSettings.put("min_term_freq", 1);
        filterSettings.put("max_term_freq", 10);
        filterSettings.put("min_doc_freq", 1);
        filterSettings.put("max_doc_freq", 100);
        filterSettings.put("min_word_length", 1);
        filterSettings.put("max_word_length", 10);

        request.setFilterSettings(filterSettings);
        Map<String, String> perFieldAnalyzer = new HashMap<String,String>();
        perFieldAnalyzer.put("user", "keyword");
        request.setPerFieldAnalyzer(perFieldAnalyzer);

        request.setRealtime(false);
        request.setRouting("routing");

        TermVectorsResponse response= null;
        try {
            response = client.termvectors(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(response);
        String index1 = response.getIndex();
        System.out.println(index1);
        String type = response.getType();
        System.out.println(type);
        String id = response.getId();
        System.out.println(id);

        boolean found = response.getFound();
        System.out.println(found);

    }

}