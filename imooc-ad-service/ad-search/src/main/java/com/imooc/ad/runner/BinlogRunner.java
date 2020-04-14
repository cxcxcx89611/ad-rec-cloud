package com.imooc.ad.runner;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.imooc.ad.mysql.BinLogConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BinlogRunner implements CommandLineRunner {
    private final BinaryLogClient client;

    @Autowired
    public BinlogRunner(BinaryLogClient client) {
        this.client = client;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Coming in BinlogRunner");
        client.connect();
    }
}
