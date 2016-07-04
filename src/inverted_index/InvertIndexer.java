package inverted_index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by hadoop on 16-4-22.
 */
public class InvertIndexer {
    public static void main(String[]args) throws ClassNotFoundException, InterruptedException, IOException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.println("Usage:inverted index<int><out>");
            System.exit(2);
        }
        try {
            Job job = new Job(conf,"invert index");
            job.setJarByClass(InvertIndexer.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(InvertIndexMapper.class);
            job.setCombinerClass(SumCombiner.class);
            job.setPartitionerClass(NewPartitioner.class);
            job.setReducerClass(InvertIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            System.exit(job.waitForCompletion(true)?0:1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
