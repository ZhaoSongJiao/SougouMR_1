package edu.zju.soft.jzs.reducer;

import edu.zju.soft.jzs.entity.CircleCaculate;
import edu.zju.soft.jzs.entity.VectorQualityEntity;
import edu.zju.soft.jzs.main.SelectFirstRandomMain;
import edu.zju.soft.jzs.main.VectorTransMain;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by 11544 on 2017/11/17.
 */
public class VectorReducer extends Reducer<IntWritable,VectorQualityEntity,Text,CircleCaculate> {

    @Override
    protected void reduce(IntWritable key, Iterable<VectorQualityEntity> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        CircleCaculate caculate= SelectFirstRandomMain.theFirstCenters.get(key.get());

        while(values.iterator().hasNext())
        {
            VectorQualityEntity entity=values.iterator().next();
            caculate.addNewFlower(entity);
        }
        context.write(caculate.getThecenter().getValue(),caculate);

    }
}
