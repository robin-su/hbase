package com.atguigu.bigdata.hbase.tool;


import com.atguigu.bigdata.hbase.mapper.ScanDataMapper;
import com.atguigu.bigdata.hbase.reducer.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * @Description: java类作用描述
 * @Author: suyb@ffcs.cn
 * @CreateDate: 2019-10-18 15:06
 * @UpdateUser: suyb@ffcs.cn
 * @UpdateDate: 2019-10-18 15:06
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class HbaseMapperReduceTool implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        //作业
        Job job = Job.getInstance();
        job.setJarByClass(HbaseMapperReduceTool.class);

        //mapper
        TableMapReduceUtil.initTableMapperJob(
        "default:student",
            new Scan(),
            ScanDataMapper.class,
            ImmutableBytesWritable.class, //map输出的key类型
            Put.class, //map输出的value类型
            job

        );

        //reducer
        TableMapReduceUtil.initTableReducerJob(
            "atguigu:user",
                InsertDataReducer.class,
                job
        );

        // 执行
        boolean flag = job.waitForCompletion(true);

        return flag ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
