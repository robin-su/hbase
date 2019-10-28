package com.atguigu.bigdata.hbase.mapper;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;

/**
 * @Description: java类作用描述
 * @Author: suyb@ffcs.cn
 * @CreateDate: 2019-10-18 15:21
 * @UpdateUser: suyb@ffcs.cn
 * @UpdateDate: 2019-10-18 15:21
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class ScanDataMapper extends TableMapper<ImmutableBytesWritable, Put> {

    /**
     *
     * @param key hbase中的rowkey
     * @param result
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        //运行Mapper查询数据
        // scan result => put
        Put put = new Put(key.get());

        for (Cell cell : result.rawCells()) {
            put.addColumn(
                    CellUtil.cloneFamily(cell),
                    CellUtil.cloneQualifier(cell),
                    CellUtil.cloneValue(cell)
            );
        }
        context.write(key,put);

    }
}
