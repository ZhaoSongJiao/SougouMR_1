package edu.zju.soft.jzs.mapper;

import edu.zju.soft.jzs.entity.SoGouData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by 11544 on 2017/11/15.
 */
public class SoGouCountMapper extends Mapper<LongWritable,Text,Text,SoGouData> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line=value.toString();
        String[] words=line.split("\t");
        SoGouData data=new SoGouData();
        try {
            data.setTs(new Text(words[0]));
            data.setUid(new Text(words[1]));
            data.setKeyword(new Text(words[2]));
            data.setRank(new IntWritable(Integer.parseInt(words[3])));
            data.setOrder(new IntWritable(Integer.parseInt(words[4])));
            data.setUrl(new Text(words[5]));
            data.setYear(new IntWritable(Integer.parseInt(words[6])));
            data.setMonth(new IntWritable(Integer.parseInt(words[7])));
            data.setDay(new IntWritable(Integer.parseInt(words[8])));
            data.setHour(new IntWritable(Integer.parseInt(words[9])));
            data.setMinutes(new IntWritable(Integer.parseInt(words[10])));
            data.setSecond(new IntWritable(Integer.parseInt(words[11])));
            context.write(data.getKeyword(),data);

        }
        catch (Exception ex)
        {
            System.out.println("数据:"+line+"可能有问题");
        }
        /*super.map(key, value, context);*/
    }
}
