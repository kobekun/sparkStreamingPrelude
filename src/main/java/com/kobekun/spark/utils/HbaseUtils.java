package com.kobekun.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Hbase操作工具类：java工具类建议采用单例模式封装
 */
public class HbaseUtils {


    HBaseAdmin admin = null;

    Configuration conf = null;


    //构造器私有
    private HbaseUtils(){
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop000:2181");
        conf.set("hbase.rootdir", "hdfs://hadoop000:8020/hbase");

        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HbaseUtils instance = null;

    public static synchronized HbaseUtils getInstance(){

        if(instance == null){
            instance = new HbaseUtils();
        }

        return instance;
    }

    public HTable getTable(String tableName){

        HTable table = null;

        try {

            table = new HTable(conf,tableName);

        } catch (IOException e) {

            e.printStackTrace();

        }

        return table;
    }

    /**
     * 添加一条记录到HBASE中
     * @param tableName HBASE表名
     * @param rowkey HBASE的rowkey
     * @param cf HBASE的columnfamily
     * @param column HBASE表的列
     * @param value 表的值
     */
    public void put(String tableName, String rowkey, String cf, String column, String value){

        HTable table = getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowkey));

        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        try {

            table.put(put);

        } catch (IOException e) {

            e.printStackTrace();

        }
    }
    public static void main(String[] args) {

//        HTable table = HbaseUtils.getInstance().getTable("imooc_course_clickcount");
//
//        System.out.println(table.getName().getNameAsString());

        String tableName = "imooc_course_clickcount";
        String rowkey = "20181111_88";
        String cf = "info";
        String column = "click_count";
        String value = "5";

        HbaseUtils.getInstance().put(tableName, rowkey, cf, column, value);
    }
}
