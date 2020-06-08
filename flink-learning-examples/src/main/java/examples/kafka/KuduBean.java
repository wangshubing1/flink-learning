package examples.kafka;

import java.util.List;

/**
 * @Author: king
 * @Date: 2019-09-09
 * @Desc: TODO
 */

public class KuduBean {
    private String _id;
    private String sharding_yyyymm;
    private String express_company;
    private String express_no;
    private String opt_time;
    private String opt_type;
    private String company_id;
    private String opt_desc;
    private String opt_user;
    private String opt_tel;
    private String courier;
    private String courier_tel;
    private String data_zone;
    private String create_time;
    private String update_time;
    private int del_tag;
    private String ods_update_time;
    private List<String> colList;

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public String getSharding_yyyymm() {
        return sharding_yyyymm;
    }

    public void setSharding_yyyymm(String sharding_yyyymm) {
        this.sharding_yyyymm = sharding_yyyymm;
    }

    public String getExpress_company() {
        return express_company;
    }

    public void setExpress_company(String express_company) {
        this.express_company = express_company;
    }

    public String getExpress_no() {
        return express_no;
    }

    public void setExpress_no(String express_no) {
        this.express_no = express_no;
    }

    public String getOpt_time() {
        return opt_time;
    }

    public void setOpt_time(String opt_time) {
        this.opt_time = opt_time;
    }

    public String getOpt_type() {
        return opt_type;
    }

    public void setOpt_type(String opt_type) {
        this.opt_type = opt_type;
    }

    public String getCompany_id() {
        return company_id;
    }

    public void setCompany_id(String company_id) {
        this.company_id = company_id;
    }

    public String getOpt_desc() {
        return opt_desc;
    }

    public void setOpt_desc(String opt_desc) {
        this.opt_desc = opt_desc;
    }

    public String getOpt_user() {
        return opt_user;
    }

    public void setOpt_user(String opt_user) {
        this.opt_user = opt_user;
    }

    public String getOpt_tel() {
        return opt_tel;
    }

    public void setOpt_tel(String opt_tel) {
        this.opt_tel = opt_tel;
    }

    public String getCourier() {
        return courier;
    }

    public void setCourier(String courier) {
        this.courier = courier;
    }

    public String getCourier_tel() {
        return courier_tel;
    }

    public void setCourier_tel(String courier_tel) {
        this.courier_tel = courier_tel;
    }

    public String getData_zone() {
        return data_zone;
    }

    public void setData_zone(String data_zone) {
        this.data_zone = data_zone;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(String update_time) {
        this.update_time = update_time;
    }

    public int getDel_tag() {
        return del_tag;
    }

    public void setDel_tag(int del_tag) {
        this.del_tag = del_tag;
    }

    public String getOds_update_time() {
        return ods_update_time;
    }

    public void setOds_update_time(String ods_update_time) {
        this.ods_update_time = ods_update_time;
    }

    public List<String> getColList() {
        colList.add("_id");
        colList.add("express_company");
        colList.add("express_no");
        colList.add("opt_time");
        colList.add("opt_type");
        colList.add("company_id");
        colList.add("opt_desc");
        colList.add("opt_user");
        colList.add("opt_tel");
        colList.add("courier");
        colList.add("courier_tel");
        colList.add("data_zone");
        colList.add("create_time");
        colList.add("update_time");
        colList.add("del_tag");
        return colList;
    }

    public void setColList(List<String> colList) {
        this.colList = colList;
    }



}
