package examples.kafka;

/**
 * @Author: king
 * @Date: Create in 2021/4/15
 * @Desc: TODO
 */
public class tbl_order_seller {
    private String id;
    private String source_no;
    private String kafka_create_time;
    private String ods_create_time;
    private String Type;

    public String getType() {
        return Type;
    }

    public void setType(String type) {
        this.Type = type;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSource_no() {
        return source_no;
    }

    public void setSource_no(String source_no) {
        this.source_no = source_no;
    }

    public String getKafka_create_time() {
        return kafka_create_time;
    }

    public void setKafka_create_time(String kafka_create_time) {
        this.kafka_create_time = kafka_create_time;
    }

    public String getOds_create_time() {
        return ods_create_time;
    }

    public void setOds_create_time(String ods_create_time) {
        this.ods_create_time = ods_create_time;
    }


    public tbl_order_seller(String id, String source_no, String kafka_create_time, String ods_create_time, String type) {
        this.id = id;
        this.source_no = source_no;
        this.kafka_create_time = kafka_create_time;
        this.ods_create_time = ods_create_time;
        this.Type = type;
    }

    @Override
    public String toString() {
        return "tbl_order_seller{" +
                "id='" + id + '\'' +
                ", source_no=" + source_no +
                ", kafka_create_time='" + kafka_create_time + '\'' +
                ", ods_create_time=" + ods_create_time +
                ", op_type=" + Type +
                '}';
    }


}
