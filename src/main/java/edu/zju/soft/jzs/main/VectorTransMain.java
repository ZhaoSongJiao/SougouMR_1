package edu.zju.soft.jzs.main;

import edu.zju.soft.jzs.service.TransData;

import java.util.LinkedHashMap;

/**
 * Created by 11544 on 2017/11/17.
 */
public class VectorTransMain {

    public static LinkedHashMap<String, Long> valuemap;


    public static void transVectorJob(String input, String output) {

    }

    public static void main(String[] args) throws Exception {
        System.out.println("开始初始化序列");
        valuemap = TransData.transFileToMap("D:\\hadoopdata\\2017-11-16-10-52-38-096sogoucount_split__out\\part-r-00000");
        System.out.println("序列初始化完成\n开始执行转换MR");
//        boolean flag = TransData.transDataToVector("G:\\hadoopdata\\sogou.full.utf8\\sogou.full.utf8", "G:\\hadoopdata\\sogou.full.utf8\\jzsvectordata", valuemap);
        boolean flag = TransData.transDataToVector("G:\\hadoopdata\\sogou.10k.utf8", "G:\\hadoopdata\\jzs10kvectordata_new3", valuemap);

        if(flag)
        {
            System.out.println("执行结束");
            System.exit(0);
        }
        else {
            System.out.println("未能正确执行");
            System.exit(1);
        }
    }

}
