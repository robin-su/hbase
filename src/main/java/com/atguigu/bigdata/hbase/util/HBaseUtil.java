package com.atguigu.bigdata.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @Description: java类作用描述
 * @Author: suyb@ffcs.cn
 * @CreateDate: 2019-10-18 21:59
 * @UpdateUser: suyb@ffcs.cn
 * @UpdateDate: 2019-10-18 21:59
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class HBaseUtil {

    //ThreadLocal:用于存储线程的本地存储,注意每个线程的本地存储是独立的，因此多线程过来
    //时，每个线程的缓存都是不一样。
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();

    private HBaseUtil() {}

    /**
     * 获取Hbase连接对象
     *   工具类一般不做异常处理，谁调我谁来处理异常
     * @return
     */
    public static void makeHbaseConnection() throws Exception{
        Connection conn = connHolder.get(); //首次还没放入ThreadLocal<Connection>则conn是空的
        if(conn == null) { //首次要创建连接对象，并写入到connHolder中
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
    }

    /**
     * 关闭连接
     * @throws Exception
     */
    public static void close() throws Exception{
        // 只要两个方法在同一个线程当中缓存ThreadLocal是共享的
        Connection conn = connHolder.get();
        if(conn != null) {
            if(!conn.isClosed()) {
                conn.close();
                connHolder.remove(); //使用完之后要将本地缓存清掉，因为conn已经close掉了，没用了
            }
        }
    }

    /**
     * 增加数据
     * @param rowKey
     * @param family
     * @param column
     * @param value
     * @throws Exception
     */
    public static void insertData(String tableName,String rowKey,String family,String column,String value)
            throws Exception {

        Connection conn = connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(family),
                      Bytes.toBytes(column),
                      Bytes.toBytes(value)
                      );

        System.out.println(tableName+"\t"+rowKey +"\t"+family +"\t" + column+"\t" + value);

        table.put(put);

        table.close();
    }

    /**
     * 生成分区键
     * @return
     */
    public static byte[][] genRegionKeys(int regionCount) {
        byte[][] bs = new byte[regionCount-1][];

        //3 ==> 2 ==> 0,1
        for(int i=0;i<regionCount -1;i++) {
            bs[i] = Bytes.toBytes(i + "|");
        }
        return bs;
    }

    /**
     * 生成分区号,
     * @param rowkey 原始分区
     * @param regionCount 分区数量
     * @return 分区号+"_" + rowkey
     */
    public static String genRegionNum(String rowkey,int regionCount) {

        int regionNum = 0;
        int hash = rowkey.hashCode();

        if(regionCount > 0 && (regionCount & (regionCount - 1)) == 0) {
            // 2^n
            regionNum = hash & (regionCount - 1);
        } else {
            regionNum = hash % regionCount; // 3个分区，则分区键盘为2个，分区键为0,1，-∞到0，0到1，1到+∞, 分区号为0，1，2
        }

        return regionNum + "_" + rowkey;
    }

    public static void main(String[] args) {
//        String zhangsan = genRegionNum("lisi", 3);
//        System.out.println(zhangsan);
        byte[][] bytes = genRegionKeys(6);
        for (byte[] aByte : bytes) {
            // 0|,1|,2|,3|,4|
            System.out.println(Bytes.toString(aByte));
        }
    }

}
