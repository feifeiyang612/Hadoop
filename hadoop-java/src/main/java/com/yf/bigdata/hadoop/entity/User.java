package com.yf.bigdata.hadoop.entity;

import lombok.Data;

/**
 * @Author: YangFei
 * @Description:
 * @create: 2019-12-13 10:49
 */
@Data
public class User {

    private int id;
    private String username;
    private String password;
    private String mobile;
    private String email;
    private String createTime;
    private String updateTime;
    private int status;

}
