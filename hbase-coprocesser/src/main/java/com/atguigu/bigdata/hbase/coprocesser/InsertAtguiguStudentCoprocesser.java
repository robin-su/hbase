package com.atguigu.bigdata.hbase.coprocesser;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * student中插入一条数据atguigu:student也插入一条
 * @Description: 协处理器
 * 1) 创建类，继承BaseRegionObserver
 * 2）重写方法 postPut
 * 3）实现逻辑：
 *      增加Student数据
 *      同时增加atguigu:student中数据
 * 4）将项目打包(依赖)后上传到hbase中（/opt/module/hbase-1.3.1/lib），让Hbase可以识别我们的协处理器
 *
 * @Author: suyb@ffcs.cn
 * @CreateDate: 2019-10-20 20:59
 * @UpdateUser: suyb@ffcs.cn
 * @UpdateDate: 2019-10-20 20:59
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class InsertAtguiguStudentCoprocesser extends BaseRegionObserver {

    /**
     * prePut：put之前
     * doPut: put时
     * postPut: put之后
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        // 获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf("atguigu:student"));

        // 增加数据
        table.put(put);

        //关闭表
        table.close();

    }
}
