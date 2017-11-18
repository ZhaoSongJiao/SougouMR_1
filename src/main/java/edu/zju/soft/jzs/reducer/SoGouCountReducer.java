package edu.zju.soft.jzs.reducer;

import edu.zju.soft.jzs.entity.CountEntity;
import edu.zju.soft.jzs.entity.SoGouData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 11544 on 2017/11/15.
 */
public class SoGouCountReducer extends Reducer<Text,SoGouData,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<SoGouData> values, Context context) throws IOException, InterruptedException {
        long count=0;
        for(SoGouData value:values)
        {
            count++;
        }
        CountEntity entity=new CountEntity();
        entity.setValue(key);
        entity.setTimes(count);

        context.write(key,new LongWritable(count));
        //        super.reduce(key, values, context);
    }
}
