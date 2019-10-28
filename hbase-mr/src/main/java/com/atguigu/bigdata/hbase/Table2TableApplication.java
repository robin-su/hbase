package com.atguigu.bigdata.hbase;

import com.atguigu.bigdata.hbase.tool.HbaseMapperReduceTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Description: 讲一个表中的数据写入到另外一个表：
 *               注意表结构要一致。
 * @Author: suyb@ffcs.cn
 * @CreateDate: 2019-10-18 15:01
 * @UpdateUser: suyb@ffcs.cn
 * @UpdateDate: 2019-10-18 15:01
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class Table2TableApplication {

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new HbaseMapperReduceTool(),args);

    }

}
