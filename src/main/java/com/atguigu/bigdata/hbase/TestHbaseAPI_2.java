package com.atguigu.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * @Description: java类作用描述
 * @Author: suyb@ffcs.cn
 * @CreateDate: 2019-10-20 21:17
 * @UpdateUser: suyb@ffcs.cn
 * @UpdateDate: 2019-10-20 21:17
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class TestHbaseAPI_2 {

    public static void main(String[] args) throws Exception{

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
        conf.set("hbase.zookeeper.property.clisentPort", "2181");
        conf.set("hbase.master", "hadoop101:16000");

        Connection connection = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("student");
        //删除表

        Admin admin = connection.getAdmin();
        if(admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表删除成功");
        }

        HTableDescriptor td = new HTableDescriptor(tableName);

        //增加协处理器
        td.addCoprocessor("com.atguigu.bigdata.hbase.coprocesser.InsertAtguiguStudentCoprocesser");

    }

}
