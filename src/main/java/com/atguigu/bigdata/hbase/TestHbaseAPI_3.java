package com.atguigu.bigdata.hbase;

import com.atguigu.bigdata.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Description: java类作用描述
 * @Author: suyb@ffcs.cn
 * @CreateDate: 2019-10-18 21:59
 * @UpdateUser: suyb@ffcs.cn
 * @UpdateDate: 2019-10-18 21:59
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class TestHbaseAPI_3 {

    /**
     *
     *  public class HBaseUtil {
     *
     *      private static Connection conn = null;
     *
     *      private HBaseUtil() {}
     *
     *      public static Connection getHbaseConnection() throws Exception{
     *          Configuration conf = HBaseConfiguration.create();
     *          conn = ConnectionFactory.createConnection();
     *          return conn;
     *      }
     *
     *      public static void close() throws Exception{
     *          if(conn != null) {
     *              if(!conn.isClosed()) {
     *                  conn.close();
     *              }
     *          }
     *      }
     *
     *  }
     *
     *  public static void main(String[] args) throws Exception {
     *         // 插入数据
     *         HBaseUtil.getHbaseConnection();
     *
     *         // 关闭连接
     *         HBaseUtil.close();
     * }
     *
     * 线程安全问题：
     *    当两个线程(A,B)同时访问main方法时，线程A调用HBaseUtil.getHbaseConnection方法执行逻辑期间，
     * 线程B也调用了HBaseUtil.getHbaseConnection方法，但是由于在HBaseUtil中Connection conn对象是静态的，
     * 也就是全局独一份，所以当线程A调用HBaseUtil.close()方法的时候，会将Connection conn对象close掉，这样
     * 线程B就无法执行自己的逻辑了。但是单机测试是测试不出这个效果的。因为单机永远是单线程
     *
     *   如果写成：则每次调用main方法都会创建一个Connection对象，每个线程关闭的都是自己的Connection对象，
     *   不会出现线程安全问题,但是容易造成资源浪费
     *   public class HBaseUtil {
     *
     *     private HBaseUtil() {}
     *
     *     public static Connection getHbaseConnection() throws Exception{
     *         Configuration conf = HBaseConfiguration.create();
     *         return ConnectionFactory.createConnection();
     *     }
     *
     *     public static void close(Configuration conf) throws Exception{
     *         if(conn != null) {
     *             if(!conn.isClosed()) {
     *                 conn.close();
     *             }
     *         }
     *     }
     *
     *  }
     *
     *  main方法改成：
     *      // 插入数据
     *      Connection conn = HBaseUtil.getHbaseConnection();
     *
     *      // 关闭连接
     *      HBaseUtil.close(conn);
     *
     *
     * 最佳解决方式：
     *      使用ThreadLocal，用于存储线程的本地存储，注意每个线程的本地存储是独立的，因此多线程过来
     *      时，每个线程的缓存都是不一样。因此从缓存中拿去自己的Connection对象不会影响其他线程
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 创建连接
        HBaseUtil.makeHbaseConnection();

        // 增加数据
        HBaseUtil.insertData("student","1002","info","name","lisi1002");

        // 关闭连接
        HBaseUtil.close();

        System.out.println("close the hbase connection successfull");
    }

}
