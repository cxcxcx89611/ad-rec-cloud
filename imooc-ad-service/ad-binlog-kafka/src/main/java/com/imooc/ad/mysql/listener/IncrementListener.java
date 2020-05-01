package com.imooc.ad.mysql.listener;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.imooc.ad.constant.Constant;
import com.imooc.ad.constant.OpType;
import com.imooc.ad.dto.BinlogRowData;
import com.imooc.ad.dto.MySqlRowData;
import com.imooc.ad.dto.TableTemplate;
import com.imooc.ad.sender.ISender;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class IncrementListener implements Ilistener {

    @Resource(name = "indexSender")
    private ISender sender;

    private final AggregationListener aggregationListener;

    @Autowired
    public IncrementListener(AggregationListener aggregationListener) {
        this.aggregationListener = aggregationListener;
    }

    @Override
    @PostConstruct
    public void register() {
        log.info("increment listener register db and table info");
        Constant.table2Db.forEach((k, v) -> aggregationListener.register(v, k, this));
    }

    @Override
    public void onEvent(BinlogRowData eventData) {
        TableTemplate table = eventData.getTable();
        EventType eventType = eventData.getEventType();

        MySqlRowData mySqlRowData = new MySqlRowData();
        mySqlRowData.setTableName(table.getTableName());
        mySqlRowData.setLevel(eventData.getTable().getLevel());
        OpType opType = OpType.to(eventType);
        mySqlRowData.setOpType(opType);

        List<String> filedList = table.getOpTypeFieldSetMap().get(opType);
        if(null == filedList) {
            log.warn("{} not support for {}", opType, table.getTableName());
            return;
        }
        for(Map<String, String> afterMap : eventData.getAfter()) {
            Map<String, String> _afterMap = new HashMap<>();
            for(Map.Entry<String, String> entry: afterMap.entrySet()) {
                String colName = entry.getKey();
                String colValue = entry.getValue();
                _afterMap.put(colName,colValue);
            }
            mySqlRowData.getFieldValueMap().add(_afterMap);
        }
        sender.sender(mySqlRowData);
    }
}
