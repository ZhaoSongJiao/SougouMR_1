package edu.zju.soft.jzs.main;

import edu.zju.soft.jzs.entity.CircleCaculate;
import edu.zju.soft.jzs.entity.VectorQualityEntity;
import edu.zju.soft.jzs.mapper.VectorMapper;
import edu.zju.soft.jzs.mapper.WordSplitMapper;
import edu.zju.soft.jzs.reducer.VectorReducer;
import edu.zju.soft.jzs.reducer.WordSplitReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

/**
 * Created by 11544 on 2017/11/17.
 */
public class SelectFirstRandomMain {

    public static ArrayList<CircleCaculate> theFirstCenters;


    public static ArrayList<CircleCaculate> initTheFirstCenter()throws Exception
    {
        String inputPath="D:\\hadoopdata\\2017-11-16-10-52-38-096sogoucount_split__out\\part-r-00000";
        File file=new File(inputPath);
        int i=0;
        ArrayList<CircleCaculate> firstcenters=new ArrayList<CircleCaculate>();
        Scanner in = new Scanner(file, "UTF-8");
        //抽取前3000个点作为我们使用的
        while(i<3000)
        {
            String line=in.nextLine();
            String[] linedata=line.split("\t");
            String keyword=linedata[0];
            String X=linedata[1];
            long Xvalue=Long.parseLong(X);
            VectorQualityEntity entity=new VectorQualityEntity();
            entity.setValue(new Text(keyword));
            //初始化将中心点放到前面的数据上
            entity.setX(new LongWritable(Xvalue*9973));
            entity.setY(new LongWritable(Xvalue));
            firstcenters.add(new CircleCaculate(entity));
            i++;
        }
        return firstcenters;
    }


    public static boolean doTheFirstJob(String inputpath,String outputpath) throws Exception
    {
        Configuration conf = new Configuration();
        Job wcjob = Job.getInstance(conf);
        wcjob.setJarByClass(SelectFirstRandomMain.class);
        wcjob.setMapperClass(VectorMapper.class);
        wcjob.setReducerClass(VectorReducer.class);
        wcjob.setMapOutputKeyClass(IntWritable.class);
        wcjob.setMapOutputValueClass(VectorQualityEntity.class);
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(CircleCaculate.class);
        FileInputFormat.setInputPaths(wcjob, new Path(inputpath));
        FileOutputFormat.setOutputPath((wcjob),new Path(outputpath));
        boolean res=wcjob.waitForCompletion(true);
        return res;
    }

    public static void main(String[] args) throws Exception
    {
        theFirstCenters=initTheFirstCenter();
        String inputfile="G:\\hadoopdata\\sogou.full.utf8\\jzsvectordata";
        //String inputfile="G:\\hadoopdata\\jzs10kvectordata_new3";
        String outputfile="D:\\hadoopdata\\";
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
        String outputfilename=sdf.format(new Date())+"_vectorcount_jzsvectorfirst";
        outputfile=outputfile+outputfilename;
        doTheFirstJob(inputfile,outputfile);
    }
}
