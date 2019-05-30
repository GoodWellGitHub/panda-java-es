package com.org.java.es.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class User {
    private String userId;
    private String userName;
    private int age;

    public static User getUser() {
        return User.builder().userId("guava").userName("李时珍").age(58).build();
    }
}
