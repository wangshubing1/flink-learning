package examples.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @Author: king
 * @Date: 2019-09-10
 * @Desc: TODO
 */

public class Utils {
    /**
     * 时间戳转为"yyyy-MM-dd HH:mm:ss"格式
     * @param time
     * @return
     */
    public static String timeToDate(String time){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date(Long.valueOf(time)));
    }

    /**
     * 获取系统当前时间
     * @return
     */
    public static String timeStamp(){
        long time = System.currentTimeMillis();
        return timeToDate(String.valueOf(time));
    }

    /**
     * 求两个list差集
     * @param list1
     * @param list2
     * @return
     */
    public static List<String> listDifferenceSet(List list1, List list2) {
        list1.removeAll(list2);
        return list1;
    }

    public static Map<String,Object> json2Map(Map<String,Object> map, String str){
        map = JSONObject.parseObject(str);
        return map;
    }
}
