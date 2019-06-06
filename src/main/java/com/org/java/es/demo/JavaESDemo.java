package com.org.java.es.demo;

import com.org.java.es.domain.User;
import com.org.java.es.util.ESUtil;

import java.util.HashMap;

public class JavaESDemo {
    public static void main(String[] args) {
        try {
            ESUtil.init();


            //ESUtil.createIndex("user_index");
            // ESUtil.delIndex("employee");
            //ESUtil.existsIndex("employee");
            //ESUtil.existsIndex("user_index");
            //String getString = ESUtil.getIndex("user_index");
            //System.out.println(getString);

            User user = User.getUser();
            // ESUtil.insertDocument(user, "user_index", "user_type", "1");

            //User user = (User) ESUtil.getDocument("user_index", "user_type", "1", User.class);
            //System.out.println(user);

            boolean exists = ESUtil.existsDocument("user_index", "user_type", "1");
            System.out.println(exists);

            //    String id=ESUtil.updateDocument(user, "user_index", "user_type", "1");
            //   System.out.println(id);

/*            HashMap<String, Object> stringStringHashMap = new HashMap<String, Object>();
            stringStringHashMap.put("userId", "guava");

            ESUtil.search(stringStringHashMap, "user_index", "user_type", null);*/


            ESUtil.close();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
