package com.atguigu.bigdata.hbase.reducer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @Description: java类作用描述
 * @Author: suyb@ffcs.cn
 * @CreateDate: 2019-10-18 15:25
 * @UpdateUser: suyb@ffcs.cn
 * @UpdateDate: 2019-10-18 15:25
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class InsertDataReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        //运行Reducer，增加数据
        for (Put put : values) {
            context.write(NullWritable.get(),put);
        }

    }
}
