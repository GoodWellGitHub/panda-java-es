package com.org.java.es.util;

import com.google.common.collect.Maps;
import com.org.java.es.domain.User;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class ElasticSearchUtil {
    private static TransportClient transPortClient;
    private static String esIP = "127.0.0.1";
    private static int esPort = 9200;

    public static void init() throws UnknownHostException {
        //设置ES实例的名称.put("client.transport.sniff", true) //自动嗅探整个集群的状态，把集群中其他ES节点的ip添加到本地的客户端列表中
        Settings esSettings = Settings.builder().put("cluster.name", "cluster.name").build();
        transPortClient = new PreBuiltTransportClient(esSettings);//初始化client较老版本发生了变化，此方法有几个重载方法，初始化插件等。
        transPortClient.addTransportAddress(new TransportAddress(InetAddress.getByName(esIP), esPort));
        System.out.println("############连接建立成功############3");
    }

    public static void createIndex() {
        for (int i = 50; i < 1000; i++) {
            User user = User.builder().userName("用户 " + i).userId("user_" + i).age(i % 100).build();
  /*          IndexResponse indexResponse = transPortClient.prepareIndex("users", "user", i + "")
                    .setSource(generateJson(user)).execute().actionGet();*/
            IndexResponse indexResponse = transPortClient.prepareIndex("users", "user", i + "")
                    .setSource(getDataMap(user)).execute().actionGet();
            System.out.println("result---" + i + indexResponse.toString());
        }
        System.out.println("do it finished");
    }

    public static String generateJson(User user) {
        String jsonUser = "";
        try {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
            Field[] fields = User.class.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                xContentBuilder.field(field.getName(), field.get(user));
            }
            jsonUser = xContentBuilder.endObject().toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonUser;
    }

    public static Map<String,Object> getDataMap(User user){
        Map<String,Object> userMap= Maps.newHashMap();
        Field[] fields=user.getClass().getDeclaredFields();
        for (Field field:fields){
            field.setAccessible(true);
            try {
                userMap.put(field.getName(),field.get(user));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return userMap;
    }
}
