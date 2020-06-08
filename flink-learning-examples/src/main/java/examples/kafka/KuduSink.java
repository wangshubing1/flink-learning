package examples.kafka;

import com.king.kudu.connector.KuduColumnInfo;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author: king
 * @Date: 2019-09-09
 * @Desc: TODO
 */

public class KuduSink {
    static String masterAddr = "10.9.251.32";
    static KuduClient client = new KuduClient.KuduClientBuilder(masterAddr)
            .defaultSocketReadTimeoutMs(6000).build();
    public static void  insertRow(Map<String,Object> col) throws KuduException {

        KuduTable table = client.openTable("impala::king_db.king_kudu");
        KuduSession kuduSession = client.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setMutationBufferSpace(3000);
        Insert insert = table.newInsert();
        insert.getRow().addString("_id", col.get("_id").toString());
        insert.getRow().addString("sharding_yyyymm",col.get("sharding_yyyymm").toString());
        insert.getRow().addString("express_company",col.get("express_company").toString());
        insert.getRow().addString("express_no",col.get("express_no").toString());
        insert.getRow().addString("opt_time",col.get("opt_time").toString());
        insert.getRow().addString("opt_type",col.get("opt_type").toString());
        insert.getRow().addString("company_id",col.get("company_id").toString());
        insert.getRow().addString("opt_desc",col.get("opt_desc").toString());
        insert.getRow().addString("opt_user",col.get("opt_user").toString());
        insert.getRow().addString("opt_tel",col.get("opt_tel").toString());
        insert.getRow().addString("courier",col.get("courier").toString());
        insert.getRow().addString("courier_tel",col.get("courier_tel").toString());
        insert.getRow().addString("data_zone",col.get("data_zone").toString());
        insert.getRow().addString("create_time",col.get("create_time").toString());
        insert.getRow().addString("update_time",col.get("update_time").toString());
        insert.getRow().addInt("del_tag",(Integer) col.get("del_tag"));
        insert.getRow().addString("ods_update_time",col.get("ods_update_time").toString());
        kuduSession.flush();
        kuduSession.apply(insert);
        kuduSession.close();
        client.close();
    }

    public static void main(String[] args) throws KuduException {
    }
}
