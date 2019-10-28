package com.atguigu.bigdata.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * @Description: java类作用描述
 * @Author: suyb@ffcs.cn
 * @CreateDate: 2019-10-19 00:16
 * @UpdateUser: suyb@ffcs.cn
 * @UpdateDate: 2019-10-19 00:16
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class TestHbaseAPI_4 {

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
		conf.set("hbase.zookeeper.property.clisentPort", "2181");
		conf.set("hbase.master", "hadoop101:16000");

        Connection connection = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("student");

        Scan scan = new Scan();

//        scan.addFamily(Bytes.toBytes("info")); 读取某个列族的数据

        // 查处rowkey比2001大的数
        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("2001"));

        //从开始到结束只有三个字符
        RegexStringComparator rsc = new RegexStringComparator("^\\d{3}$");

//        Filter f = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,bc);
        Filter f = new RowFilter(CompareFilter.CompareOp.EQUAL,rsc);

        // FilterList.Operator.MUST_PASS_ALL : and
        // FilterList.Operator.MUST_PASS_ONE : or
        //ColumnPaginationFilter 是分页过滤器
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE); // and 所有的条件都必须满足

        RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL,bc);

        list.addFilter(f);
        list.addFilter(rf);

        // 扫描时增加过滤器
        // 所谓的过滤，其实每条数据都会筛选过滤，性能比较低
        scan.setFilter(list);

        Table table = connection.getTable(tableName);

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("value=" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("rowkey=" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family=" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column=" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            }
        }

        table.close();
        connection.close();

    }

}
