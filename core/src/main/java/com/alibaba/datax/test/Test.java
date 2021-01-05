package com.alibaba.datax.test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

public class Test {
    public static void main(String[] args){
        String info="dcxcxcvz[IDM_PARA.aaabbb]fffasdfas";
       String data = info.substring(info.indexOf("[IDM_PARA."),info.lastIndexOf("]")+1);
       System.out.println(data);
       info = info.replace(data,"_FFF_FFFF");
       System.out.println(info);


    }
}
