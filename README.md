# MapReduce output stored as Parquet using Kite APIs
### MRWordCountKite

This is an example of using Kite APIs to write a Parquet file as an output of a MapReduce program. Kite provides simple APIs with a clean abstraction to read and write files in different file formats (Parquet, Avro etc) across Hadoop. Please see
[Kite SDK Dataset URIs](http://kitesdk.org/docs/1.0.0/URIs.html) for more info on the supported URIs.

This example uses the canonical MapReduce word count example to demonstrate how the results of the word count can be written out as a Parquet file. 

Here is a high level description of the program:
* In order to store data as Parquet, we use a simple WordCount.avsc Avro schema and use Avro-tools to convert it into a Java class:
```
{
 "type": "record",
 "name": "Words",
 "namespace": "com.cloudera.sa",
 "fields": [
     {"name": "word", 	"type": "string"},
     {"name": "count",  "type": "int"}
 ]
}
```
* The Mapper and Reducer classes are both pretty straight forward with the exception that the Reducer writes out Words objects as the Key and Void as the value. The *DatasetKeyOutputFormat* used to write out the key/value pairs ignores the Value. 
```
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
```
* The driver class follows this high level pattern to configure the output:
  * Define a DatasetDescriptor using the appropriate _Schema_, _CompressionType_ and _Format_
  ```
    DatasetDescriptor datasetDescriptor = new DatasetDescriptor.Builder()
                                              .schema(schema)
                                              .format(format)
                                              .compressionType(compressionType)
                                              .build();
  ```
  * Create a Dataset using the DatasetDescriptor object.
  * Configure the DatasetKeyOutputFormat using the Dataset object
  ```
  DatasetKeyOutputFormat.configure(job).writeTo(datasets).withType(Words.class);
  ```
As we can see, the Kite API provides tremendous flexibility in switching between storage _Formats_, _CompressionTypes_ and destination _URIs_ with very little change in the code.
