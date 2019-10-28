package com.atguigu.bigdata.hbase;

import com.atguigu.bigdata.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Description: java类作用描述
 * @Author: suyb@ffcs.cn
 * @CreateDate: 2019-10-21 21:53
 * @UpdateUser: suyb@ffcs.cn
 * @UpdateDate: 2019-10-21 21:53
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class TestPrePartition {

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
        conf.set("hbase.zookeeper.property.clisentPort", "2181");
        conf.set("hbase.master", "hadoop101:16000");

        Connection connection = ConnectionFactory.createConnection(conf);

        //增加数据
        Table empTable = connection.getTable(TableName.valueOf("emp1"));

        String rowKey = "zhangsan"; //一定放到一号分区中

        //hashMap
        //将rowkey均匀的分配到不同的区域中，效果和HashMap数据存储的规则是一样的

        //HashMap
        rowKey = HBaseUtil.genRegionNum(rowKey,3);

        Put put = new Put(Bytes.toBytes(rowKey));


        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("20"));

        empTable.put(put);

    }

    private static void createPrePartitionTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();

        HTableDescriptor td = new HTableDescriptor(TableName.valueOf("emp1"));

        HColumnDescriptor cd = new HColumnDescriptor("info");

        td.addFamily(cd);

        byte[][] bs = new byte[2][];

        // 预分区 -∞ 0 1 +∞ 希望得到0开头的插入0号分区，1开头的插入1号分区
        // 如果分区号是0，则一般的字符串都会比0大，所以很难有数据，|竖线是ASCII码中第二大的值，}第一大
        bs[0] = Bytes.toBytes("0|");
        bs[1] =Bytes.toBytes("1|");


        admin.createTable(td,bs);

        System.out.println("创建成功");
    }

}
