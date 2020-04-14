package com.imooc.ad.service;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class BinLogServiceTest {
    public static void main (String args[]) throws Exception{
        System.out.println("start");
        BinaryLogClient client = new BinaryLogClient("127.0.0.1", 3306, "root", "12345678");
        client.registerEventListener(event -> {
            EventData data = event.getData();
            System.out.println("start3");
            if( data instanceof UpdateRowsEventData){
                System.out.println("update ----------");
                System.out.println(data.toString());
            } else if (data instanceof WriteRowsEventData){
                System.out.println("Write ----------");
                System.out.println(data.toString());
            }else if (data instanceof DeleteRowsEventData){
                System.out.println("Delete ----------");
                System.out.println(data.toString());
            }
        });
        client.connect();
    }
}
