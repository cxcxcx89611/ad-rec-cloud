package com.imooc.ad.sender.index;

import com.alibaba.fastjson.JSON;
import com.imooc.ad.dump.table.AdCreativeTable;
import com.imooc.ad.dump.table.AdPlanTable;
import com.imooc.ad.handler.AdLevelDataHandler;
import com.imooc.ad.index.DataLevel;
import com.imooc.ad.mysql.constant.Constant;
import com.imooc.ad.mysql.dto.MySqlRowData;
import com.imooc.ad.sender.ISender;
import com.imooc.ad.utils.CommonUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component("indexSender")
public class IndexSender implements ISender {
    @Override
    public void sender(MySqlRowData rowData) {
        String level = rowData.getLevel();
        if (DataLevel.LEVEL2.getLevel().equals(level)) {
            Level2RowData(rowData);
        } else if (DataLevel.LEVEL3.getLevel().equals(level)) {

        } else if (DataLevel.LEVEL4.getLevel().equals(level)) {

        } else {
            log.error("MysqlRowData error: {}", JSON.toJSONString(rowData));
        }
    }

    private void Level2RowData(MySqlRowData rowData) {
        if (rowData.getTableName().equals(Constant.AD_PLAN_TABLE_INFO.TABLE_NAME)) {
            List<AdPlanTable> adPlanTableList = new ArrayList<>();
            for (Map<String, String> filedValueMap : rowData.getFieldValueMap()) {
                AdPlanTable adPlanTable = new AdPlanTable();
                filedValueMap.forEach((k, v) -> {
                    switch (k) {
                        case Constant.AD_PLAN_TABLE_INFO.COLUMN_ID:
                            adPlanTable.setId(Long.valueOf(v));
                            break;
                        case Constant.AD_PLAN_TABLE_INFO.COLUMN_USER_ID:
                            adPlanTable.setUserId(Long.valueOf(v));
                            break;
                        case Constant.AD_PLAN_TABLE_INFO.COLUMN_PLAN_STATUS:
                            adPlanTable.setPlanStatus(Integer.valueOf(v));
                            break;
                        case Constant.AD_PLAN_TABLE_INFO.COLUMN_START_DATE:
                            adPlanTable.setStartDate(CommonUtils.parseStringDate(v));
                            break;
                        case Constant.AD_PLAN_TABLE_INFO.COLUMN_END_DATE:
                            adPlanTable.setEndDate(CommonUtils.parseStringDate(v));
                            break;
                    }
                });
                adPlanTableList.add(adPlanTable);
            }
            adPlanTableList.forEach(p -> AdLevelDataHandler.handleLevel2(p, rowData.getOpType()));
        } else if (rowData.getTableName().equals(Constant.AD_PLAN_TABLE_INFO.TABLE_NAME)) {
            List<AdCreativeTable> adCreativeTableList = new ArrayList<>();
            for (Map<String, String> filedValueMap : rowData.getFieldValueMap()) {
                AdCreativeTable adCreativeTable = new AdCreativeTable();
                filedValueMap.forEach((k, v) -> {
                    switch (k) {
                        case Constant.AD_CREATIVE_TABLE_INFO.COLUMN_ID:
                            adCreativeTable.setAdId(Long.valueOf(v));
                            break;
                        case Constant.AD_CREATIVE_TABLE_INFO.COLUMN_HEIGHT:
                            adCreativeTable.setHeight(Integer.valueOf(v));
                            break;
                        case Constant.AD_CREATIVE_TABLE_INFO.COLUMN_AUDIT_STATUS:
                            adCreativeTable.setAuditStatus(Integer.valueOf(v));
                            break;
                        case Constant.AD_CREATIVE_TABLE_INFO.COLUMN_WIDTH:
                            adCreativeTable.setWidth(Integer.valueOf(v));
                            break;
                        case Constant.AD_CREATIVE_TABLE_INFO.COLUMN_MATERIAL_TYPE:
                            adCreativeTable.setMaterialType(Integer.valueOf(v));
                            break;
                        case Constant.AD_CREATIVE_TABLE_INFO.COLUMN_TYPE:
                            adCreativeTable.setType(Integer.valueOf(v));
                            break;
                        case Constant.AD_CREATIVE_TABLE_INFO.COLUMN_URL:
                            adCreativeTable.setAdUrl(v);
                            break;
                    }
                });
                adCreativeTableList.add(adCreativeTable);
            }
            adCreativeTableList.forEach(p -> AdLevelDataHandler.handleLevel2(p, rowData.getOpType()));
        }
    }
}
