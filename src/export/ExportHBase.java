package export;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by hadoop on 16-5-19.
 */
public class ExportHBase {
    static Configuration conf = HBaseConfiguration.create();
    @SuppressWarnings("deprecation")
    public static void getResultScan(String tableName, String filePath)
            throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        HTable table =
                new HTable(conf, tableName);
        try {
            rs = table.getScanner(scan);
            FileWriter fos = new FileWriter(filePath);
            for (Result r : rs) {
                for (KeyValue keyvalue : r.raw()) {
                    String s = new String
                            (new String(keyvalue.getRow()) + "\t" +
                                    new String(keyvalue.getValue()) + "\n");fos.write(s);
                }
            }
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        rs.close();
    }
    public static void main(String[] args) throws Exception {
        String tableName = "Wuxia";
        String filePath = "/home/hadoop/桌面/AllResult.txt";
        getResultScan(tableName, filePath);
    }
}
