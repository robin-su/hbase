package com.atguigu.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * @Description: java类作用描述
 * @Author: suyb@ffcs.cn
 * @CreateDate: 2019-10-17 14:22
 * @UpdateUser: suyb@ffcs.cn
 * @UpdateDate: 2019-10-17 14:22
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class TestHbaseAPI_1 {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);
        new HBaseAdmin(connection);

    }

}
