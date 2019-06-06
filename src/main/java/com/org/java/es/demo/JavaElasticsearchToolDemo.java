package com.org.java.es.demo;

import com.org.java.es.util.ElasticsearchTool;

import java.util.HashMap;
import java.util.Map;

public class JavaElasticsearchToolDemo {
    public static void main(String[] args) {
        ElasticsearchTool tool = new ElasticsearchTool("red_floor", "1");
        ElasticsearchTool.init();
       // tool.synchronousExecution("red_floor", "1");
        //tool.asynchronousExecution();

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("name", "林黛玉");
        map.put("age", 18);
        //map.put("sex", "女");

        //tool.buildIndex("user_floor", "1", map);
        //tool.buildIndex("red_floor", null, map);

        tool.termQuery("red_floor","name","林黛玉");


        ElasticsearchTool.close();
    }
}
