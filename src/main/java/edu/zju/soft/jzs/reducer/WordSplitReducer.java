package edu.zju.soft.jzs.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 11544 on 2017/11/16.
 */
public class WordSplitReducer extends Reducer<Text,LongWritable,Text,LongWritable> {


    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count=0;
        for(LongWritable number:values)
        {
            count=count+number.get();
        }
        context.write(key,new LongWritable(count));
    }

}
