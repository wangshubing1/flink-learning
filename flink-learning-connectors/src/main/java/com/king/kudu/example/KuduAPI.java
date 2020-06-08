package com.king.kudu.example;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: king
 * @Date: 2019-08-27
 * @Desc: TODO
 */

public class KuduAPI {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KuduAPI.class);


    private static KuduClient initTable(){
        final String KUDU_MASTER = "szsjhl-cdh-test-10-9-251-32.belle.lan:7051";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
        return client;
    }
    private static void createTable(String tableName){
        try {
            logger.info("------------create start--------------");
            //创建表
            List<ColumnSchema> columns = new ArrayList(2);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
                    .build());
            List<String> rangeKeys = new ArrayList<>();
            rangeKeys.add("key");
            Schema schema = new Schema(columns);
            initTable().createTable(tableName, schema,
                    new CreateTableOptions().setRangePartitionColumns(rangeKeys));
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insertData(String tableName) throws KuduException {
        logger.info("------------insert start--------------");
        initTable().getTablesList().getTablesList().forEach(str-> System.out.println(str));
        KuduTable table = initTable().openTable(tableName);
        KuduSession session = initTable().newSession();
        session.setTimeoutMillis(60000);
        for (int i = 0; i < 3; i++) {
            logger.info("----------------insert  "+i+"---------------");
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addInt(0, i);
            row.addString(1, "value " + i);
            session.apply(insert);
        }
    }
    private void deleteData (String tableName) throws KuduException {
        logger.info("------------delete data start--------------");
        //根据主键删除数据
        KuduTable table = initTable().openTable(tableName);
        KuduSession session = initTable().newSession();
        Delete delete = table.newDelete();
        PartialRow row = delete.getRow();
        row.addInt("key",0);
        OperationResponse apply = session.apply(delete);
        if (apply.hasRowError()) {
            logger.info("------------delete fail--------------");
        } else {
            logger.info("------------delete success--------------");
        }
    }

    private void updateData (String tableName) throws KuduException {
        logger.info("------------update start--------------");
        //更新数据
        KuduTable table = initTable().openTable(tableName);
        KuduSession session = initTable().newSession();
        Update update = table.newUpdate();
        PartialRow row1 = update.getRow();
        row1.addInt("key",6);
        row1.addString("value","kexin");
        session.apply(update);
    }

    private void scanData(String tableName) throws KuduException {
        logger.info("------------scan start--------------");
        //扫描数据
        KuduTable table = initTable().openTable(tableName);
        List<String> projectColumns = new ArrayList<>(1);
        projectColumns.add("value");
        KuduScanner scanner = initTable().newScannerBuilder(table)
                .setProjectedColumnNames(projectColumns)
                .build();
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                System.out.println(result.getString(0));
            }
        }
    }

    private static void deleteTable(String tableName) throws KuduException {
        logger.info("------------delete table start--------------");
        //删除表
        initTable().deleteTable(tableName);
        initTable().shutdown();
    }

    public static void main(String[] args) throws KuduException {
        String tableName = "impala::king_db.king_kudu";
        deleteTable(tableName);
        //createTable(tableName);

    }

}
