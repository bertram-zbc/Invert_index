package inverted_index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * Created by hadoop on 16-4-22.
 */
public class InvertIndexMapper extends Mapper<Object,Text,Text,IntWritable> {

    protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        fileName=fileName.replaceAll(".txt.segmented","");
        //System.out.println(fileName);
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()){
            Text word = new Text();
            word.set(itr.nextToken()+"#"+fileName);
            context.write(word, new IntWritable(1));
        }

    }

}
