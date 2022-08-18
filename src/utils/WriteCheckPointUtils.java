package utils;

import record.CheckPointRecord;
import record.StreamRecord;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author: liuzq
 * @date: 2022/8/17
 */
public class WriteCheckPointUtils {
    static BufferedWriter writer;
    public static void writeCheckPointFile(String path, CheckPointRecord record){
        try{
            writer = new BufferedWriter(new FileWriter(path,false));
            writer.write(record.toString());
            writer.flush();
            writer.close();
        } catch (IOException e){
            System.out.println("Could not save checkpointfile wrong with uncorrect savepath");
            e.printStackTrace();
        }

    }
}
