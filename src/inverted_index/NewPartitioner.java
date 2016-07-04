package inverted_index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by hadoop on 16-4-23.
 */
public class NewPartitioner extends HashPartitioner<Text,IntWritable> {
    public int getPartition(Text key,IntWritable value,int numReduceTasks){
        String term = new String();
        term = key.toString().split("#")[0];
        return super.getPartition(new Text(term),value,numReduceTasks);
    }
}
