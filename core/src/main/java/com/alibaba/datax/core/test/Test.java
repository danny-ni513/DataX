package com.alibaba.datax.core.test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.google.gson.Gson;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.text.MessageFormat;
import java.util.Map;

public class Test {
    public static void main(String[] args){
        String str="{\n" +
                "    \"job\": {\n" +
                "        \"setting\": {\n" +
                "            \"speed\": {\n" +
                "                \"channel\": 1\n" +
                "            }\n" +
                "        },\n" +
                "        \"content\": [\n" +
                "            {\n" +
                "                \"reader\": {\n" +
                "                    \"name\": \"hbase11xreader\",\n" +
                "                    \"parameter\": {\n" +
                "\"filter\":{\n" +
                "\"value\":\"[aaa.bbb.cc]\"\n" +
                "},\n" +
                "\"nacosConfig\":\"hbaseProd\","+
                "                        \"table\": \"users\",\n" +
                "                        \"encoding\": \"utf-8\",\n" +
                "                        \"mode\": \"normal\",\n" +
                "                        \"column\": [\n" +
                "                            {\n" +
                "                                \"name\": \"rowkey\",\n" +
                "                                \"type\": \"string\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"name\": \"info: age\",\n" +
                "                                \"type\": \"string\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"name\": \"info: birthday\",\n" +
                "                                \"type\": \"date\",\n" +
                "                                \"format\":\"yyyy-MM-dd\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"name\": \"info: company\",\n" +
                "                                \"type\": \"string\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"name\": \"address: contry\",\n" +
                "                                \"type\": \"string\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"name\": \"address: province\",\n" +
                "                                \"type\": \"string\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"name\": \"address: city\",\n" +
                "                                \"type\": \"string\"\n" +
                "                            }\n" +
                "                        ],\n" +
                "                        \"range\": {\n" +
                "                            \"startRowkey\": \"\",\n" +
                "                            \"endRowkey\": \"\",\n" +
                "                            \"isBinaryRowkey\": true\n" +
                "                        }\n" +
                "                    }\n" +
                "                },\n" +
                "                \"writer\": {\n" +
                "                    \"name\": \"txtfilewriter\",\n" +
                "                    \"parameter\": {\n" +
                "                        \"path\": \"/Users/shf/workplace/datax_test/hbase11xreader/result\",\n" +
                "                        \"fileName\": \"qiran\",\n" +
                "                        \"writeMode\": \"truncate\"\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "}";

        Configuration configuration = Configuration.from(str);

        Configuration readConfig = configuration.getConfiguration(CoreConstant.JOB_CONTENT_0_READER_PARAMETER);
        String tmpConfigStr="  {\"hbaseConfig[0].jdbc\":{\"hbase.zookeeper.quorum\":[\"xxxx\"]},\"username\":\"1111\"}";
        Gson gson = new Gson();
        Map<String,Object> map = gson.fromJson(tmpConfigStr,Map.class);
        map.keySet().stream().forEach(key->{
            System.out.println(key);
            readConfig.set(key,map.get(key));
        });
        configuration.set(CoreConstant.JOB_CONTENT_0_READER_PARAMETER,readConfig);
        System.out.println(configuration);

        String readerNacosConfig = configuration.getString(CoreConstant.JOB_CONTENT_0_READER_PARAMETER+"."+CoreConstant.NACOS_CONFIG);

        String dataId = readerNacosConfig.substring(1,readerNacosConfig.length());
        System.out.println(dataId);

    }
}
