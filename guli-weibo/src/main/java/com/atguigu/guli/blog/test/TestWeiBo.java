package com.atguigu.guli.blog.test;

import com.atguigu.guli.blog.constants.Constants;
import com.atguigu.guli.blog.dao.HBaseDao;
import com.atguigu.guli.blog.utils.HBaseUtils;

import java.io.IOException;

/**
 * @Description: java类作用描述
 * @Author: suyb@ffcs.cn
 * @CreateDate: 2019-10-26 21:02
 * @UpdateUser: suyb@ffcs.cn
 * @UpdateDate: 2019-10-26 21:02
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class TestWeiBo {

    public static void init() {


        try {
            // 创建命名空间
            HBaseUtils.createNameSpace(Constants.NAME_SPACE);

            // 创建微博内容表
            HBaseUtils.createTable(Constants.CONTENT_TABLE,Constants.CONTENT_TABLE_VERSIONS,Constants.CONTENT_TABLE_CF);

            // 创建用户关系表
            HBaseUtils.createTable(Constants.RELATION_TABLE,
                    Constants.RELATION_TABLE_VERSIONS,
                    Constants.RELATION_TABLE_CF1,
                    Constants.RELATION_TABLE_CF2);

            // 创建收件箱表
            HBaseUtils.createTable(Constants.INBOX_TABLE,Constants.INBOX_TABLE_VERSIONS,Constants.INBOX_TABLE_CF);

        } catch (Exception e) {
            e.printStackTrace();
        }



    }

    public static void main(String[] args) throws Exception {

        //初始化
        init();

        //1001发布微博
        HBaseDao.publishWeibo("1001","赶紧下课吧！！！");

        //1002关注1001和1003
        HBaseDao.addAttends("1002","1001","1003");

        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("*************1111***************");

        //1003发布3条微博，同事1001发布2条微博
        HBaseDao.publishWeibo("1003","谁说的赶紧下课！！！");
        Thread.sleep(10);
        HBaseDao.publishWeibo("1001","没说话！！！");
        Thread.sleep(10);
        HBaseDao.publishWeibo("1003","那谁说的！！！");
        Thread.sleep(10);
        HBaseDao.publishWeibo("1001","反正飞机是下线了！！！");
        Thread.sleep(10);
        HBaseDao.publishWeibo("1003","你们爱咋咋地！！！");

        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("*************222***************");

        //1002取消关注1003
        HBaseDao.deleteAttends("1002","1003");
        System.out.println("*************333***************");

        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("*************444***************");

        //1002再次关注1003
        HBaseDao.addAttends("1002","1003");

        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("*************555***************");

        //获取1001的微博详情
        HBaseDao.getWeibo("1001");

    }

}
