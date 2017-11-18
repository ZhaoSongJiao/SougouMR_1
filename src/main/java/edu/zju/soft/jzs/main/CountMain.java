package edu.zju.soft.jzs.main;

import edu.zju.soft.jzs.entity.CountEntity;
import edu.zju.soft.jzs.entity.SoGouData;
import edu.zju.soft.jzs.mapper.OrderMapper;
import edu.zju.soft.jzs.mapper.SoGouCountMapper;
import edu.zju.soft.jzs.reducer.OrderReducer;
import edu.zju.soft.jzs.reducer.SoGouCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Created by 11544 on 2017/11/15.
 */
public class CountMain {


    public static boolean firstJob(String inputpath,String outputpath) throws Exception {
        System.out.println("正在处理第一JOB");
        Configuration conf = new Configuration();
        Job wcjob = Job.getInstance(conf);
        wcjob.setJarByClass(CountMain.class);
        wcjob.setMapperClass(SoGouCountMapper.class);
        wcjob.setReducerClass(SoGouCountReducer.class);
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(SoGouData.class);
        //设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(LongWritable.class);
        //指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob, new Path(inputpath));
        FileOutputFormat.setOutputPath((wcjob),new Path(outputpath));
        boolean res = wcjob.waitForCompletion(true);
        if(res)
        {
            System.out.println("数据MR正常处理完成");
        }
        else
        {
            System.out.println("数据可能正确的没有处理完成");
        }
        return res;
    }

    public static boolean orderJob(String inputpath,String outputpath) throws Exception
    {
        System.out.println("正在处理统计排序的JOB");
        Configuration conf = new Configuration();
        Job wcjob = Job.getInstance(conf);
        wcjob.setJarByClass(CountMain.class);
        wcjob.setMapperClass(OrderMapper.class);
        wcjob.setReducerClass(OrderReducer.class);

        wcjob.setMapOutputKeyClass(CountEntity.class);
        wcjob.setMapOutputValueClass(Text.class);
        //设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(LongWritable.class);
        //指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob, new Path(inputpath));
        FileOutputFormat.setOutputPath((wcjob),new Path(outputpath));
        boolean res = wcjob.waitForCompletion(true);

        return res;
    }

    public static void main(String[] args) throws Exception
    {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
        String outputfilename=sdf.format(new Date())+"sogoucount";
        String dir="D:\\hadoopdata\\";
        String outputfile=dir+outputfilename;
        String inputfile="G:\\hadoopdata\\sogou.full.utf8\\jzssogoudata";
        boolean flag=firstJob(inputfile,outputfile);
        if(flag)
        {
            System.out.println("开始执行第2个JOB");
            boolean judge=orderJob(outputfile+"\\part-r-00000",outputfile+"__out");
                    if(judge)
                        System.exit(0);
        }
        System.exit(1);
    }

}
