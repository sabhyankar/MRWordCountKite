package com.cloudera.sa;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.*;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;



/**
 * Created by Sameer Abhyankar on 7/19/15.
 */
public class MRWordCountDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new GenericOptionsParser(new Configuration(),args).getConfiguration();
        int ret = ToolRunner.run(conf, new MRWordCountDriver(), args);
        System.exit(ret);
    }


    public int run(String[] args) throws Exception {
        if (args.length != 2 ) {
            System.err.printf("Usage: $s [generic options] <input path> <output path> \n", getClass().getName());
            GenericOptionsParser.printGenericCommandUsage(System.err);
            System.exit(-1);
        }


        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());


        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Create a Kite HDFS uri based on the output path specified.
        String kiteOutURI = "dataset:hdfs:" + args[1].trim();

        Dataset<Words> datasets = createDataset(Words.getClassSchema(),CompressionType.Snappy,kiteOutURI,Formats.PARQUET,Words.class);

        // Use DatasetKeyOutputFormat to write the reducer kv pairs
        // DatasetKeyOutputFormat ignores the value.
        DatasetKeyOutputFormat.configure(job).writeTo(datasets).withType(Words.class);


        job.setMapperClass(MRWordCountMapper.class);
        job.setReducerClass(MRWordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public <T> Dataset<T> createDataset(Schema schema, CompressionType compressionType, String uri, Format format,Class<T> aClass) {
        DatasetDescriptor datasetDescriptor = new DatasetDescriptor.Builder()
                                            .schema(schema)
                                            .format(format)
                                            .compressionType(compressionType)
                                            .build();
        return Datasets.create(uri,datasetDescriptor,aClass);
    }

}
