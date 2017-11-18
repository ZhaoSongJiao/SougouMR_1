package edu.zju.soft.jzs.reducer;

import edu.zju.soft.jzs.entity.CountEntity;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 11544 on 2017/11/15.
 */
public class OrderReducer extends Reducer<CountEntity,Text,Text,LongWritable> {
    @Override
    protected void reduce(CountEntity key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key.getValue(),new LongWritable(key.getTimes()));
    }
}
