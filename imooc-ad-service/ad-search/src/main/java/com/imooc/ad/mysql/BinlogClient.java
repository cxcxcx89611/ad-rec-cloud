package com.imooc.ad.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.imooc.ad.mysql.listener.AggregationListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
public class BinlogClient {
    private BinaryLogClient client;

    private final BinLogConfig config;

    private final AggregationListener listener;

    @Autowired
    public BinlogClient(BinLogConfig config, AggregationListener listener) {
        this.config = config;
        this.listener = listener;
    }

    public void connect() {
        new Thread(() -> {
            client = new BinaryLogClient(config.getHost(),
                    config.getPort(),
                    config.getUserName(),
                    config.getPassword()
            );
            if (!StringUtils.isEmpty(config.getBinLogName()) && !config.getPosition().equals(-1)) {
                client.setBinlogFilename(config.getBinLogName());
                client.setBinlogPosition(config.getPosition());
            }
            client.registerEventListener(listener);
            try {
                log.info("BinLog client start");
                client.connect();
                log.info("connect to mysql done!");
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }).start();
    }

    public void close() {
        try {
            client.disconnect();
        }catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
