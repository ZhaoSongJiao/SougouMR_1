package edu.zju.soft.jzs.main;

import edu.zju.soft.jzs.mapper.WordSplitMapper;
import edu.zju.soft.jzs.reducer.WordSplitReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by 11544 on 2017/11/16.
 */
public class SplitMain {


    public static boolean firstJob(String inputpath,String outputpath) throws Exception
    {
        Configuration conf = new Configuration();
        Job wcjob = Job.getInstance(conf);
        wcjob.setJarByClass(SplitMain.class);
        wcjob.setMapperClass(WordSplitMapper.class);
        wcjob.setReducerClass(WordSplitReducer.class);
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(LongWritable.class);
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(wcjob, new Path(inputpath));
        FileOutputFormat.setOutputPath((wcjob),new Path(outputpath));
        boolean res=wcjob.waitForCompletion(true);
        return res;
    }


    public static void main(String[] args) throws Exception {
        String inputfilepath="D:\\hadoopdata\\2017-11-16-10-52-38-096sogoucount__out\\part-r-00000";
        String outputfiledir="D:\\hadoopdata\\2017-11-16-10-52-38-096sogoucount_split";
        boolean flag=firstJob(inputfilepath,outputfiledir);
        if(flag)
        {
            System.out.println("完成了分词，准备进行分词统计");
            boolean judge=CountMain.orderJob(outputfiledir+"\\part-r-00000",outputfiledir+"__out");
            if(judge) {
                System.out.println("执行完成");
                System.exit(0);
            }
        }
        System.exit(1);
    }






}
