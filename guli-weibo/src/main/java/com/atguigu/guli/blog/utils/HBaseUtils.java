package com.atguigu.guli.blog.utils;

import com.atguigu.guli.blog.constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * 1.创建命名空间
 * 2.判断表是否存在
 * 3.创建表（三张表）
 */
public class HBaseUtils {


    /**
     * 创建表的命名空间
     * @param nameSpace 命名空间名字
     * @throws Exception
     */
    public static void createNameSpace(String nameSpace) throws Exception {
        //1.获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //2.获取Admin对象
        Admin admin = connection.getAdmin();

        //3.构建命名空间描述器
        NamespaceDescriptor nameSpaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        //4.创建命名空间
        admin.createNamespace(nameSpaceDescriptor);

        //5.关闭资源
        admin.close();
        connection.close();
    }

    /**
     * 判断表是否存在。注意为了体现封装性，这里将其定义为私有
     * @param tableName
     * @return
     * @throws Exception
     */
    private static boolean isTableExist(String tableName) throws Exception{

        //1.获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //2.获取Admin对象
        Admin admin = connection.getAdmin();

        //3.执行判断是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        //4.关闭资源
        admin.close();
        connection.close();

        return exists;
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param versions 版本号
     * @param cfs 列族数组
     * @throws Exception
     */
    public static void createTable(String tableName,int versions,String ... cfs) throws Exception{
        //1.判断是否传入了列族信息
        if(cfs.length <= 0) {
            System.out.println("请设置列族信息");
            return;
        }

        //2.判断表是否存在
        if(isTableExist(tableName)) {
            System.out.println(tableName + "表已存在");
            return;
        }

        //3.获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //4.获取Admin对象
        Admin admin = connection.getAdmin();
        //5.创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //6.循环添加列族信息
        for (String cf : cfs) {

            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

            //7.设置版本
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }


        //8.创建表操作
        admin.createTable(hTableDescriptor);

        //9.关闭资源
        admin.close();
        connection.close();

    }


}
