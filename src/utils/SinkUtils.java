package utils;

import record.StreamRecord;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-14
 */
//Sink算子写入文件工具类
public class SinkUtils {
    static BufferedWriter writer;
    public static void writeIntoFile(String path, StreamRecord<?> data) throws IOException {
        writer = new BufferedWriter(new FileWriter(path,true));
        Object value = data.getValue();
        writer.append(value.toString());
        writer.newLine();
        writer.close();
    }
    public static void writeTimestamp(Timestamp startTime, Timestamp endTime) {
        String start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(startTime.getTime()));
        String end = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(endTime.getTime()));
        String timeInfo = start + " - " + end;
        //往result写入新的时间窗口
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("output/wordcount.txt", true));
            bw.append(timeInfo);
            bw.newLine();
            bw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
