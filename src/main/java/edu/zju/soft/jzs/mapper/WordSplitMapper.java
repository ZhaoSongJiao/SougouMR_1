package edu.zju.soft.jzs.mapper;

import edu.zju.soft.jzs.entity.SoGouData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import java.io.IOException;
import java.util.List;

/**
 * Created by 11544 on 2017/11/16.
 */
public class WordSplitMapper extends Mapper<LongWritable,Text,Text,LongWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line=value.toString().split("\t");
        String theword=line[0];
        long times=Long.parseLong(line[1]);
        List<Word> words= WordSegmenter.seg(theword);
        for(Word word:words)
        {
            context.write(new Text(word.getText()),new LongWritable(times));
        }
    }

}
