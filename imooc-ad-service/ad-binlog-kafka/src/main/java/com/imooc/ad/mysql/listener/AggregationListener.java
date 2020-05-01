package com.imooc.ad.mysql.listener;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.imooc.ad.mysql.TemplateHolder;
import com.imooc.ad.dto.BinlogRowData;
import com.imooc.ad.dto.TableTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class AggregationListener implements BinaryLogClient.EventListener {

    private String dbName;
    private String tableName;

    private Map<String, Ilistener> listenerMap = new HashMap<>();

    private final TemplateHolder templateHolder;


    @Autowired
    public AggregationListener(TemplateHolder templateHolder) {
        this.templateHolder = templateHolder;
    }

    private String genKey(String dbName, String tableName) {
        return dbName + ":" + tableName;
    }

    public void register(String _dbName, String _tableName, Ilistener ilistener) {
        listenerMap.put(genKey(dbName,tableName), ilistener);
    }

    @Override
    public void onEvent(Event event) {
        EventType type = event.getHeader().getEventType();
        log.debug("event type: {}", type);

        if(type == EventType.TABLE_MAP) {
            TableMapEventData data = event.getData();
            this.tableName =  data.getTable();
            this.dbName = data.getDatabase();
        }
        if(type != EventType.EXT_UPDATE_ROWS
                && type != EventType.EXT_WRITE_ROWS
                && type != EventType.EXT_DELETE_ROWS) {
            return;
        }
        if(StringUtils.isEmpty(dbName) || StringUtils.isEmpty(tableName)) {
            log.error("no metata data event");
            return;
        }

        String key = genKey(this.dbName, this.tableName);
        Ilistener ilistener = this.listenerMap.get(key);

        if(null == ilistener) {
            log.debug("skip {}", key);
            return;
        }

        log.info("trigger event: {}", type.name());

        try {
            BinlogRowData rowData = buildRowData(event.getData());
            if(rowData == null) {
                return;
            }
            rowData.setEventType(type);
            ilistener.onEvent(rowData);
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error(ex.getMessage());
        } finally {
            this.dbName = "";
            this.tableName = "";
        }
    }


    private List<Serializable[]> getAfterValues(EventData data) {
        if(data instanceof WriteRowsEventData){
            return ((WriteRowsEventData) data).getRows();
        }
        if(data instanceof UpdateRowsEventData){
            return ((UpdateRowsEventData) data).getRows().stream().map(Map.Entry::getValue).collect(Collectors.toList());
        }
        if(data instanceof DeleteRowsEventData){
            return ((DeleteRowsEventData) data).getRows();
        }
        return Collections.EMPTY_LIST;
    }

    private BinlogRowData buildRowData(EventData eventData) {
        TableTemplate table = this.templateHolder.getTable(tableName);
        if (null == table) {
            log.warn("table {} not found", tableName);
            return null;
        }
        List<Map<String, String>> afterMapList = new ArrayList<>();
        for(Serializable[] after : getAfterValues(eventData)){
            Map<String, String> afterMap = new HashMap<>();
            int colLen = after.length;
            for (int ix = 0; ix<colLen; ix++){
                String colName = table.getPosMap().get(ix);
                if (null == colName) {
                    log.debug("ignore position: {}", ix);
                    continue;
                }
                String colValue = after[ix].toString();
                afterMap.put(colName, colValue);
            }
            afterMapList.add(afterMap);
        }
        BinlogRowData rowData = new BinlogRowData();
        rowData.setAfter(afterMapList);
        rowData.setTable(table);

        return rowData;
    }
}
