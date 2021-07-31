package com.hajimei.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HajimeiHBase {

//    public static final String ZK_CONNECT_KEY = "hbase.zookeeper.quorum";
//    public static final String ZK_CONNECT_VALUE = "localhost";//standalone
//    public static final String ZK_CONNECT_VALUE = "47.101.xx.xx:2181,47.101.xx.xx:2181,47.101.xx.xx:2181";

    public static HBaseAdmin admin = null;
    private static Configuration config = null;
    private static Connection connection = null;

    public static void main(String[] args) throws Exception {
        config = HBaseConfiguration.create();
        // config.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
        config.set("hbase.zookeeper.quorum", "47.101.xx.xx");// zookeeper address
        config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper port

        connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        // create namespace
        // NamespaceDescriptor.Builder nsd = NamespaceDescriptor.create("hajimei");
        // nsd.addConfiguration("hajimei","999");
        // NamespaceDescriptor build = nsd.build();
        // admin.createNamespace(build);


        String studentName = "hajimei:student";
        String[] families = {"info", "score"};

        //test data for inserting data
        List<Put> puts = new ArrayList<>();
        Put put = new Put(Bytes.toBytes("初め"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("G20210735010164"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("xx"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("xx"));
        puts.add(put);

        crateTable(studentName, families, admin);
        initTable(studentName);
        queryTable(studentName, "初め");
        scanTable(studentName);
        deleteRow(studentName, "Tom");
        scanTable(studentName);
        insertData(studentName, puts);
        queryTable(studentName, "初め");
        deleteQualifier(studentName, "Jerry", "info", "class");
        scanTable(studentName);
        deleteColumnFamily(studentName, "score", admin);
        scanTable(studentName);
        dropTable(studentName, admin);

        // drop namespace
        // admin.deleteNamespace("hajimei");

        admin.close();
        connection.close();
    }

    public static void crateTable(String studentName, String[] families, Admin admin) throws IOException {
        TableName studentTable = TableName.valueOf(studentName);

        if (admin.tableExists(studentTable)) {
            System.out.println("Table " + studentName + " exists");

            TableName[] tableNames = admin.listTableNames();
            System.out.println("List all tables:");
            for (TableName t : tableNames) {
                System.out.println(t);
            }
        } else {
            // build column family
            List<ColumnFamilyDescriptor> descriptorList = new ArrayList<>();
            for (String cf : families) {
                ColumnFamilyDescriptor descriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf))
                        .setDataBlockEncoding(DataBlockEncoding.PREFIX)
                        .setBloomFilterType(BloomType.ROW)
                        .build();
                descriptorList.add(descriptor);
            }

            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(studentTable)
                    .setColumnFamilies(descriptorList)
                    .build();

            // create table
            admin.createTable(tableDescriptor);
        }

    }

    public static void initTable(String studentName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(studentName));
        List<Put> puts = new ArrayList<>();

        Put put = new Put(Bytes.toBytes("Tom"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000001"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("75"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("82"));
        puts.add(put);

        put = new Put(Bytes.toBytes("Jerry"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000002"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("85"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("67"));
        puts.add(put);

        put = new Put(Bytes.toBytes("Jack"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000003"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("80"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("80"));
        puts.add(put);

        put = new Put(Bytes.toBytes("Rose"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000004"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("60"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("61"));
        puts.add(put);

        table.put(puts);
        System.out.println("init table completed");

    }

    public static void queryTable(String studentName, String rowKey) throws IOException {
        // Get row or cell contents; pass table name, row, and optionally a dictionary of column(s), timestamp, time range and versions.
        System.out.println("Query table via Get method:");
        Table table = connection.getTable(TableName.valueOf(studentName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);

        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("student_id"))));
        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("class"))));

        System.out.println(result);
    }

    public static void scanTable(String studentName) throws Exception {
        System.out.println("Scan whole table:");
        Table table = connection.getTable(TableName.valueOf(studentName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            System.out.println(result);
        }
    }

    public static void insertData(String studentName, List<Put> puts) throws IOException {
        System.out.println("Insert data:");
        Table table = connection.getTable(TableName.valueOf(studentName));
        table.put(puts);
    }

    public static void deleteRow(String studentName, String rowKey) throws IOException {
        System.out.println("Delete data " + rowKey + " :");
        Table table = connection.getTable(TableName.valueOf(studentName));
        Delete delete=new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    public static void deleteQualifier(String studentName, String rowKey, String cfName, String qualifier) throws IOException {
        System.out.println("Delete qualifier " + cfName + "-" + qualifier + " :");
        Table table = connection.getTable(TableName.valueOf(studentName));
        Delete delete=new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier));
        table.delete(delete);
    }

    public static void deleteColumnFamily(String studentName, String cfName, Admin admin) throws IOException {
        System.out.println("Delete column family " + cfName + " :");
        admin.deleteColumnFamily(TableName.valueOf(studentName),Bytes.toBytes(cfName));
    }

    public static void dropTable(String studentName, Admin admin) throws IOException {
        System.out.println("Drop table:");
        admin.disableTable(TableName.valueOf(studentName));
        admin.deleteTable(TableName.valueOf(studentName));
    }

}
