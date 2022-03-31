import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.core.ResponseInputStream;

public class WordCount {

  public static class Map 
            extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1); // type of output value
    private Text word = new Text();   // type of output key
    private ArrayList<String> allowedWords = new ArrayList<String>();

    public void setup(Context context) throws IOException, InterruptedException {
        try {
          Configuration conf = context.getConfiguration();
          String bucketName = conf.get("bucketName");
          String objectKey = conf.get("objectKey");
          Region region = Region.US_EAST_1;

          S3Client s3 = S3Client.builder()
                  .region(region)
                  .build();

          GetObjectRequest request = GetObjectRequest.builder()
                  .bucket(bucketName)
                  .key(objectKey)
                  .build();

          ResponseInputStream<GetObjectResponse> s3objectResponse = s3.getObject(request);
          BufferedReader reader = new BufferedReader(new InputStreamReader(s3objectResponse));

          String line;
          while ((line = reader.readLine()) != null) {
            allowedWords.add(line);
          }

          reader.close();
          s3.close();

        } catch (Exception e) {
          System.err.println(e.getMessage());
          System.exit(1);
        }
    }
      
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString()); // line to string token

      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();
        if (allowedWords.contains(token)) {
          word.set(token);    // set word as each input keyword
          context.write(word, one);     // create a pair <keyword, 1>
        }
      }
    }
  }
  
  public static class Reduce
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0; // initialize the sum for each keyword
      for (IntWritable val : values) {
        sum += val.get();  
      }
      result.set(sum);

      context.write(key, result); // create a pair <keyword, number of occurences>
    }
  }

  // Driver program
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
    if (otherArgs.length != 3) {
      System.err.println("Usage: WordCount <in> <out> <dict.txt>");
      System.exit(2);
    }
    conf.setStrings("bucketName", otherArgs[2]);
    conf.setStrings("objectKey", "dict/dict.txt");

    // create a job with name "wordcount"
    Job job = new Job(conf, "wordcount");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
   
    // uncomment the following line to add the Combiner
    job.setCombinerClass(Reduce.class);

    // set output key type   
    job.setOutputKeyClass(Text.class);
    // set output value type
    job.setOutputValueClass(IntWritable.class);
    //set the HDFS path of the input data
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    // set the HDFS path for the out
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    //Wait till job completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
