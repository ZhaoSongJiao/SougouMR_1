package edu.zju.soft.jzs.mapper;

import edu.zju.soft.jzs.entity.CircleCaculate;
import edu.zju.soft.jzs.entity.VectorQualityEntity;
import edu.zju.soft.jzs.main.SelectFirstRandomMain;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by 11544 on 2017/11/17.
 */
public class VectorMapper extends Mapper<LongWritable,Text,IntWritable,VectorQualityEntity> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line=value.toString().split("\t");
        VectorQualityEntity entity=new VectorQualityEntity();
        entity.setValue(new Text(line[0]));
        entity.setX(new LongWritable(Long.parseLong(line[1])));
        entity.setY(new LongWritable(Long.parseLong(line[2])));
        entity.setTimes(new LongWritable(1));

        try {
            int i=0;
            int length = SelectFirstRandomMain.initTheFirstCenter().size();
            int lowdistanceflag=0;
            double lowdistance=100000000000000000.0;
            for (i=0;i<length;i++)
            {
                CircleCaculate caculate=SelectFirstRandomMain.theFirstCenters.get(i);
                double far=caculate.getDistanceToThisCenter(entity);
                if(far<lowdistance)
                {
                    lowdistance=far;
                    lowdistanceflag=i;
                }
            }

            context.write(new IntWritable(lowdistanceflag),entity);

        }
        catch (Exception e)
        {
            System.out.println("获取不到数据了");
        }
    }
}
