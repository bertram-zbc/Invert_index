package inverted_index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 16-4-22.
 */
public class InvertIndexReducer extends Reducer<Text,IntWritable,Text,Text> {

    private Text word1 = new Text();
    private Text word2 = new Text();
    static Text CurrentItem = new Text(" ");//记录当前的key用来与下一个key比较
    static List<String> postingList = new ArrayList<String>();

    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int sum=0;
        word1.set(key.toString().split("#")[0]);
        for (IntWritable val:values){
            sum+=val.get();
        }
        word2.set(key.toString().split("#")[1]+":"+sum);
        if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")){
            //不相同说明已经是下一个词了，需要把当前得到的所有文档和词频取出来
            StringBuilder out = new StringBuilder();
            long count=0;//记录出现在所有文件中的总次数
            for (String p:postingList){
                count+=Long.parseLong(p.substring(p.indexOf(":")+1,p.length()));
            }

            if (count>0){
                //计算平均出现次数
                out.append("\t");
                out.append((double) count/postingList.size());
                out.append(",");
                //记录文档信息
                for (String p:postingList){
                    out.append(p);
                    out.append(";");
                }
                context.write(CurrentItem,new Text(out.toString()));
                postingList = new ArrayList<String>();
            }

        }
        CurrentItem = new Text(word1);
        postingList.add(word2.toString());
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        StringBuilder out = new StringBuilder();
        long count = 0;//记录出现在所有文件中的总次数
        for (String p : postingList) {
            count += Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
        }

        if (count > 0) {
            //计算平均出现次数
            out.append("\t");
            out.append( (double) count / postingList.size());
            out.append(",");
            //记录文档信息
            for (String p : postingList) {
                out.append(p);
                out.append(";");
            }
            context.write(CurrentItem, new Text(out.toString()));
        }
    }
}

