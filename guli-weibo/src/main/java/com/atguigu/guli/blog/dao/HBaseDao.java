package com.atguigu.guli.blog.dao;

import com.atguigu.guli.blog.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 1.发布微博
 * 2.删除微博
 * 3.关注微博
 * 4.取关用户
 * 5.获取用户微博详情
 * 6.获取用户初始化页面
 */
public class HBaseDao {

    private static long maxTimeStamp = 9999999999999l;

    //1.发布微博
    public static void publishWeibo(String uid,String content) throws IOException {
        //获取Connection对象(可以定义连接池来做）
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //第一部分：操作微博内容表
        //1.获取微博内容表对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //2.获取当前时间戳
        long ts = maxTimeStamp - System.currentTimeMillis();
        //3.获取rowKey
        String rowkey = uid + "_" + ts;
        //4.创建Put对象
        Put contPut = new Put(Bytes.toBytes(rowkey));
        //5.给Put对象赋值
        contPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF),Bytes.toBytes("content"),Bytes.toBytes(content));
        //6.执行插入数据
        contTable.put(contPut);
        //第二部分：操作微博收件箱表，首先从关注关系表中查询出B对应的粉丝的RowKey,然后再根据Rowkey对收件箱表进行添加
        // 1.获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        // 2.获取当前发布微博人的fancs列族数据
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
        Result result = relationTable.get(get); //从关系表中找出粉丝

        // 3.创建一个集合，用户存放微博内容表的Put对象
        ArrayList<Put> inboxPuts = new ArrayList<>();

        //4. 遍历粉丝
        for (Cell cell : result.rawCells()) {
            //5. 构建微博收件表的Put对象
            System.out.println(new String(CellUtil.cloneQualifier(cell)));
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), //列族
                    Bytes.toBytes(uid), //列
                    Bytes.toBytes(rowkey)); //值 可以定位到微博内容表对应的微博
            inboxPuts.add(inboxPut);
        }

        //6. 给收件箱表的Put对象赋值
        if(inboxPuts.size() > 0) {
            //获取收件箱对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

            //执行收件箱表数据插入操作
            inboxTable.put(inboxPuts);

            //关闭收件箱表
            inboxTable.close();
        }

        //7.关闭资源
        relationTable.close();
        contTable.close();
        connection.close();
    }

    //2.关注用户
    public static void addAttends(String uid,String ... attends) throws Exception{

        if(attends.length <= 0) {
            System.out.println("请选择要关注的人！！！");
        }

        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //第一部分：操作用户关系表
        //1.获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        //2.创建一个集合，用户存放用户关系表的Put对象
        List<Put> relaPuts = new ArrayList<>();

        //3.创建操作者的Put的对象
        Put uidPut = new Put(Bytes.toBytes(uid));

        //4.循环创建被关注者的Put的对象
        for(String attend:attends) {
            //5.给操作者的Put对象赋值，关注者uid一个Put可以存放多个列
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1),
                    Bytes.toBytes(attend),
                    Bytes.toBytes(attend));

            //对于被关注者，每个被关注者需要被
            Put attendPut = new Put(Bytes.toBytes(attend));
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2),
                    Bytes.toBytes(uid),
                    Bytes.toBytes(uid));

            relaPuts.add(attendPut);
        }

        relaPuts.add(uidPut);

        //第二部分：操作收件箱表
        //1.获取微博内容表的对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        //2.创建收件箱表的Put对象
        Put inboxPut = new Put(Bytes.toBytes(uid));

        //3.循环attends，获取每个被关注者的近期发布的微博
        for(String attend : attends) {
            //"|"的ASCAII码最大
            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            //3.1 获取当前关注者的近期发布的微博(scan)-> 集合ResultScanner
            ResultScanner resultScanner = contTable.getScanner(scan);

            long ts = System.currentTimeMillis();

            for (Result result : resultScanner) {
                //给收件箱表的Put对象赋值
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF),
                        Bytes.toBytes(attend),
                        ts ++, //防止统一时刻发了两条微博，而没有被显示出来
                        result.getRow());
            }
        }


        if(!inboxPut.isEmpty()) {
            // 获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            //插入数据
            inboxTable.put(inboxPut);
            //关闭收件箱表链接
            inboxTable.close();
        }

        relationTable.close();
        contTable.close();
        connection.close();

    }

    //3.取消关注
    public static void deleteAttends(String uid,String ...dels ) throws Exception {

        if(dels.length <= 0) {
            System.out.println("请添加待取关的用户！！！");
            return;
        }

        //1.获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //2.第一部分：操作用户关系表
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        ArrayList<Delete> relaDeletes = new ArrayList<>();

        Delete uidDelete = new Delete(Bytes.toBytes(uid));

        for(String del : dels) {
            //关注者，取消其关注的对象
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1),Bytes.toBytes(del));

            //被关注者取消关注者这个粉丝
            Delete delDelete = new Delete(Bytes.toBytes(del));
            delDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2),Bytes.toBytes(uid));
            relaDeletes.add(delDelete);
        }
        relaDeletes.add(uidDelete);
        relaTable.delete(relaDeletes);

        //3.第二部分：操作收件箱表
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));

        for(String del : dels) {
            inboxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF),Bytes.toBytes(del));
        }
        inboxTable.delete(inboxDelete);

        relaTable.close();
        inboxTable.close();
        connection.close();
    }


    //4.获取初始化页面
    public static void getInit(String uid) throws Exception {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);
        for (Cell cell : result.rawCells()) {
            //获取收件箱表中uid对应的粉丝发表的微博id
            Get contGet = new Get(CellUtil.cloneValue(cell));

            Result contResult = contTable.get(contGet);

            for (Cell contCell : contResult.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(contCell)) +
                        ",CF" + Bytes.toString(CellUtil.cloneFamily(contCell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(contCell)) +
                        ",Value" + Bytes.toString(CellUtil.cloneValue(contCell)));
            }
        }
        inboxTable.close();
        contTable.close();
        connection.close();
    }


    //5.获取某个人的所有微博详情
    public static void getWeibo(String uid) throws Exception {

        //1.获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //2.获取微博内容表
        Table table = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //3.构建Scan对象
        Scan scan = new Scan();

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));

        scan.setFilter(rowFilter);

        ResultScanner resultScanner = table.getScanner(scan);

        for(Result result:resultScanner) {
            for(Cell cell:result.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        table.close();
        connection.close();
    }


}
