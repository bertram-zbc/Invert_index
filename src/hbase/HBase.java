package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by hadoop on 16-5-12.
 */
public class HBase {
    //HBase的配置设置
    private static Configuration config = null;
    private static Connection connection = null;
    private static Table table=null;
    //private static String tablename=null;
    static {
        config = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
//        config.set("hbase.zookeeper.quorum", "127.0.0.1");
//        config.set("hbase.zookeeper.property.clientPort", "2181");
    }

    //创建表操作
    public void createTable(String tableName) {

        Admin admin = null;

        try {
            admin = connection.getAdmin();

            if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
                HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
                hbaseTable.addFamily(new HColumnDescriptor("book"));
                admin.createTable(hbaseTable);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }

                if (connection != null && !connection.isClosed()) {
                    //connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    //添加数据
    public static void addData(String tableName,String rowKey,String family,String qualifier,String value) throws IOException {
//        HTable table = new HTable(config,tableName);
//        Put put = new Put(Bytes.toBytes(rowKey));
//        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
//        table.put(put);
        table = connection.getTable(TableName.valueOf(tableName));
        Put p = new Put(Bytes.toBytes(rowKey));
        p.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
        table.put(p);
        table.close();
    }

    //关闭连接
    public static void closeHBase() throws IOException {
        connection.close();
        //table.close();
    }

//    public static void main(String[] args) throws IOException {
//        HBase htest = new HBase();
//        htest.createTable("test");
//        htest.addData("test","row1","family1","name","ZhangSan");
//        htest.addData("test","row1","family1","sex","male");
//        htest.addData("test","row2","family1","name","Lily");
//        htest.addData("test","row2","family1","sex","female");
//
//        connection.close();
//    }
}
