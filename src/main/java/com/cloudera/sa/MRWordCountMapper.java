package com.cloudera.sa;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Sameer Abhyankar on 7/19/15.
 */
public class MRWordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for(String word: words) {
            context.write(new Text(word),ONE);
        }
    }
}
