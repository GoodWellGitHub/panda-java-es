package com.org.java.es.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

public class ESUtil {
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

    public static void createIndex(String index) throws Exception {
        CreateIndexRequest request = new CreateIndexRequest(index);//创建索引
        //创建的每个索引都可以有与之关联的特定设置。
        request.settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 2));
        //可选参数
        request.setTimeout(TimeValue.timeValueMinutes(2));//超时,等待所有节点被确认(使用TimeValue方式)
        request.setMasterTimeout(TimeValue.timeValueMinutes(1));//连接master节点的超时时间(使用TimeValue方式)
        request.waitForActiveShards(ActiveShardCount.DEFAULT);//在创建索引API返回响应之前等待的活动分片副本的数量，以int形式表示。

        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

        System.out.println("isAcknowledged:" + createIndexResponse.isAcknowledged());
        System.out.println("isShardsAcknowledged:" + createIndexResponse.isShardsAcknowledged());
    }

    public static void delIndex(String index) throws Exception {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        request.timeout(TimeValue.timeValueMinutes(2));
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println("isAcknowledged:" + response.isAcknowledged());
    }


    public static boolean existsIndex(String index) throws Exception {
        GetIndexRequest request = new GetIndexRequest();
        request.indices(index);
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(index + "索引存在？" + exists);
        return exists;
    }

    public static String getIndex(String index) throws IOException {
        GetIndexRequest request = new GetIndexRequest().indices(index);
        GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
        return Arrays.toString(response.getIndices());
    }

    public static void insertDocument(Object object, String index, String type, String id) throws IOException, IllegalAccessException {
        Map<String, String> objectMap = objectMap(object);
        IndexRequest indexRequest = new IndexRequest(index, type, id).source(objectMap);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("创建index :" + indexResponse.getId());
    }

    public static Object getDocument(String index, String type, String id, Class targetClass) throws IOException {
        GetRequest getRequest = new GetRequest(index, type, id);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        String getResult = getResponse.getSourceAsString();
        System.out.println(getResult);
        return JSONObject.toJavaObject(JSON.parseObject(getResult), targetClass);
    }

    public static boolean existsDocument(String index, String type, String id) throws IOException {
        GetRequest getRequest = new GetRequest(index, type, id);
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        return exists;
    }

    public static void delDocument(String index, String type, String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
        deleteRequest.timeout(TimeValue.timeValueMinutes(2));
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse);
        long sepNo = deleteResponse.getSeqNo();

    }

    public static String updateDocument(Object object, String index, String type, String id) throws IOException, IllegalAccessException {
        Map<String, String> objectMap = objectMap(object);
        UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(objectMap);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        return updateResponse.getId();
    }

    public static Object search(Map<String, Object> condition, String index, String type, String id) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        for (Map.Entry entry : condition.entrySet()) {
            searchSourceBuilder.query(QueryBuilders.matchQuery((String) entry.getKey(), entry.getValue()));
        }
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchSourceBuilder.sort(new FieldSortBuilder("userId").order(SortOrder.ASC));
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        searchSourceBuilder.timeout(TimeValue.timeValueMinutes(2));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] searchHitsArr = searchHits.getHits();
        for (SearchHit searchHit : searchHitsArr) {
            Map<String, Object> stringObjectMap = searchHit.getSourceAsMap();
            System.out.println(stringObjectMap);
        }
        return null;
    }


    private static Map<String, String> objectMap(Object object) throws IllegalAccessException {
        Map<String, String> objectMap = Maps.newHashMap();
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            objectMap.put(field.getName(), JSON.toJSONString(field.get(object)));
        }
        return objectMap;
    }

    public static void closeIndex() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
