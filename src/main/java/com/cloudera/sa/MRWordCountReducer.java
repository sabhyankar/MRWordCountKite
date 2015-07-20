package com.cloudera.sa;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

/**
 * Created by sabhyankar on 7/19/15.
 */
public class MRWordCountReducer extends Reducer<Text,IntWritable,Words,Void> {


    @Override
    public void reduce(Text text, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable v: values) {
            count++;
        }
        context.write(new Words(text.toString(),count),null);
    }

}
