package edu.zju.soft.jzs.mapper;

import edu.zju.soft.jzs.entity.CountEntity;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 只是进行一下map重新通过CountEntity排序
 * Created by 11544 on 2017/11/15.
 */
public class OrderMapper extends Mapper<LongWritable,Text,CountEntity,Text> {
//    static int testcount=0;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line=value.toString().split("\t");
        try {
            long times = Integer.parseInt(line[1]);
            String str = line[0];
            CountEntity entity=new CountEntity();
            entity.setTimes(times);
            entity.setValue(new Text(str));
            context.write(entity,new Text(str));
        }
        catch (Exception ex)
        {
            System.out.println("数据lines:"+value.toString()+"有问题");
        }
    }
}
