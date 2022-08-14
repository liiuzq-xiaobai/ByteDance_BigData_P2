package utils;

import record.StreamRecord;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-14
 */
public class SinkUtils {
    static BufferedWriter writer;
    public static void writeIntoFile(String path, StreamRecord<?> data) throws IOException {
        writer = new BufferedWriter(new FileWriter(path,true));
        Object value = data.getValue();
        writer.append(value.toString());
        writer.newLine();
        writer.close();
    }
}
