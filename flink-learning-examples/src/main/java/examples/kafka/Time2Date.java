package examples.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: king
 * @Date: 2019-09-09
 * @Desc: TODO
 */

public class Time2Date {
    public static String timeToDate(String time){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date(Long.valueOf(time)));
    }

    public static String timeStamp(){
        long time = System.currentTimeMillis();
        return timeToDate(String.valueOf(time));
    }

    public static void main(String[] args) {
        System.out.println(timeStamp());
    }

}
