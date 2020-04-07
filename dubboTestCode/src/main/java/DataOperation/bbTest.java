package DataOperation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
public class bbTest {

    public static Configuration conf = HBaseConfiguration.create();

    static {
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.zookeeper.quorum",
                "ugcserver3,ugcserver4,ugcserver5");
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
        conf = HBaseConfiguration.create(HBASE_CONFIG);
    }

    public static void creatTable(String tableName, String[] familys)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok");
        }
    }

    public static void main(String[] args) {
        String tableName = "test";
        String[] fa = { "c1", "c2" };
        int i = 0;
        List<Put> list = new ArrayList<Put>();
        try {
            bbTest.creatTable(tableName, fa);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("测试开始插入数据");
        long start = System.currentTimeMillis();
        HTablePool pool = new HTablePool(conf, 1000);
        //HTableInterface table = pool.getTable(tableName);
        HTable table = (HTable) pool.getTable(tableName);
        table.setAutoFlush(false);
        try {
            table.setWriteBufferSize(24*1024*1024);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        File file = new File("/opt/pmdce/datamining/123.txt");
        BufferedReader reader = null;
        String lineString = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            while ((lineString = reader.readLine()) != null) {
                i++;
                String[] lines = lineString.split("\\,");
                Put put = new Put(lines[0].getBytes());
                put.add("c1".getBytes(), "name".getBytes(), lines[1].getBytes());
                put.add("c1".getBytes(), "age".getBytes(), lines[2].getBytes());
                put.add("c2".getBytes(), "class".getBytes(),lines[3].getBytes());
                list.add(put);
                if (i % 10000 == 0) {
                    table.put(list);
                    list.clear();
                    table.flushCommits();
                }
            }
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        long stop = System.currentTimeMillis();
        System.out.println("插入数据共耗时：" + (stop - start) * 1.0 / 1000 + "s");
    }
}