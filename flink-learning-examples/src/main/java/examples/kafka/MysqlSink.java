package examples.kafka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @Author: king
 * @Date: Create in 2021/4/15
 * @Desc: tbl_order_seller 目标样例表的bean
 */
public class MysqlSink extends RichSinkFunction<tbl_order_seller> {
    PreparedStatement replace_ps;
    PreparedStatement delete_ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DbUtils.getConnection();
        String sql = "replace into tbl_order_seller(id, source_no, kafka_create_time,ods_create_time) values(?, ?, ?, ?);";
        String delete_sql = "delete from tbl_order_seller where id = ?;";
        replace_ps = this.connection.prepareStatement(sql);
        delete_ps = this.connection.prepareStatement(delete_sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭并释放资源
        if (connection != null) {
            connection.close();
        }

        if (replace_ps != null) {
            replace_ps.close();
        }
        if (delete_ps != null) {
            delete_ps.close();
        }

    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(tbl_order_seller value, Context context) throws Exception {
        //遍历数据集合
        // for (Results employee : value) {


        replace_ps.setString(1, value.getId());
        replace_ps.setString(2, value.getSource_no());
        replace_ps.setString(3, value.getKafka_create_time());
        replace_ps.setString(4, value.getOds_create_time());
        delete_ps.setString(1, value.getId());
        if (value.getType().equalsIgnoreCase("DELETE")) {
            System.out.println("开始执行：" + value.getType() + " SQL");
            delete_ps.addBatch();
            int[] delete_count =delete_ps.executeBatch();
            System.out.println("成功了删除了" + delete_count.length + "行数据");
        } else {
            replace_ps.addBatch();
            int[] count = replace_ps.executeBatch();//批量后执行
            System.out.println("成功了插入了" + count.length + "行数据");
        }
    }
}
