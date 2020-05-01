package com.imooc.ad.handler;

import com.alibaba.fastjson.JSON;
import com.imooc.ad.index.district.UnitDistrictIndex;
import com.imooc.ad.dump.table.*;
import com.imooc.ad.index.DataTable;
import com.imooc.ad.index.IndexAware;
import com.imooc.ad.index.adplan.AdPlanIndex;
import com.imooc.ad.index.adplan.AdPlanObject;
import com.imooc.ad.index.adunit.AdUnitIndex;
import com.imooc.ad.index.adunit.AdUnitObject;
import com.imooc.ad.index.creative.CreativeIndex;
import com.imooc.ad.index.creative.CreativeObject;
import com.imooc.ad.index.creativeunit.CreativeUnitIndex;
import com.imooc.ad.index.creativeunit.CreativeUnitObject;
import com.imooc.ad.index.interest.UnitItIndex;
import com.imooc.ad.index.keyword.UnitKeywordIndex;
import com.imooc.ad.constant.OpType;
import com.imooc.ad.utils.CommonUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class AdLevelDataHandler {
    public static void handleLevel2(AdPlanTable adPlanTable, OpType type) {
        AdPlanObject planObject = new AdPlanObject(adPlanTable.getId(),adPlanTable.getUserId(),
                adPlanTable.getPlanStatus(),
                adPlanTable.getStartDate(),
                adPlanTable.getEndDate());
        handleBinLogEvent(DataTable.of(AdPlanIndex.class),planObject.getPlanId(), planObject, type);
    }

    public static void handleLevel2(AdCreativeTable adCreativeTable, OpType type) {
        CreativeObject creativeObject = new CreativeObject(
                adCreativeTable.getId(),
                adCreativeTable.getName(),
                adCreativeTable.getType(),
                adCreativeTable.getMaterialType(),
                adCreativeTable.getHeight(),
                adCreativeTable.getWidth(),
                adCreativeTable.getAuditStatus(),
                adCreativeTable.getUrl());
        handleBinLogEvent(DataTable.of(CreativeIndex.class),creativeObject.getAdId(), creativeObject, type);
    }

    public static void handleLevel3(AdUnitTable unitTable, OpType type) {
        AdPlanObject adPlanObject = DataTable.of(AdPlanIndex.class).get(unitTable.getPlanId());
        if(null==adPlanObject){
            log.error("handle level 3 found AdPlanObject error: {}", unitTable.getPlanId());
            return;
        }
        AdUnitObject unitObject = new AdUnitObject(
                unitTable.getUnitId(),
                unitTable.getUnitStatus(),
                unitTable.getPositionType(),
                unitTable.getPlanId(),
                adPlanObject
        );
        handleBinLogEvent(DataTable.of(AdUnitIndex.class),unitObject.getUnitId(), unitObject, type);
    }

    public static void handleLevel3(AdCreativeUnitTable creativeUnitTable, OpType opType) {
        if(opType == opType.UPDATE) {
            log.error("CreativeUnitIndex, not support update");
            return;
        }
        AdUnitObject adUnitObject = DataTable.of(AdUnitIndex.class).get(creativeUnitTable.getUnitId());
        CreativeObject creativeObject = DataTable.of(CreativeIndex.class).get(creativeUnitTable.getAdId());
        if(null == adUnitObject || null==creativeObject) {
            log.error("AdCreativeUnitTable index error: {}", JSON.toJSONString(creativeUnitTable));
            return;
        }
        CreativeUnitObject creativeUnitObject = new CreativeUnitObject(
                creativeUnitTable.getAdId(),
                creativeUnitTable.getUnitId()
        );
        handleBinLogEvent(DataTable.of(CreativeUnitIndex.class),
                CommonUtils.stringConcat(creativeUnitObject.getAdId().toString(),creativeUnitObject.getUnitId().toString()),
                creativeUnitObject,
                opType
                );
    }

    public static void handleLevel4(AdUnitDistrictTable adUnitDistrictTable, OpType type){
        if(type == type.UPDATE) {
            log.error("AdUnitDistrictTable, not support update");
            return;
        }
        AdUnitObject adUnitObject = DataTable.of(AdUnitIndex.class).get(adUnitDistrictTable.getUnitId());
        if(adUnitObject == null) {
            log.error("AdUnitDistrictTable index error: {}", JSON.toJSONString(adUnitDistrictTable));
            return;
        }
        String key = CommonUtils.stringConcat(adUnitDistrictTable.getProvince(), adUnitDistrictTable.getCity());
        Set<Long> value = new HashSet<>(Collections.singleton(adUnitDistrictTable.getUnitId()));
        handleBinLogEvent(DataTable.of(UnitDistrictIndex.class), key, value, type);
    }

    public static void handleLevel4(AdUnitItTable unitItTable, OpType type) {
        if(type == type.UPDATE) {
            log.error("AdUnitItTable, not support update");
            return;
        }
        AdUnitObject adUnitObject = DataTable.of(AdUnitIndex.class).get(unitItTable.getUnitId());
        if(adUnitObject == null) {
            log.error("AdUnitItTable index error: {}", JSON.toJSONString(unitItTable));
            return;
        }
        Set<Long> value = new HashSet<>(Collections.singleton(unitItTable.getUnitId()));
        handleBinLogEvent(DataTable.of(UnitItIndex.class), unitItTable.getItTag(), value, type);
    }

    public static void handleLevel4(AdUnitKeywordTable unitKeywordTable, OpType type) {
        if(type == type.UPDATE) {
            log.error("AdUnitKeywordTable, not support update");
            return;
        }
        AdUnitObject adUnitObject = DataTable.of(AdUnitIndex.class).get(unitKeywordTable.getUnitId());
        if(adUnitObject == null) {
            log.error("AdUnitKeywordTable index error: {}", JSON.toJSONString(unitKeywordTable));
            return;
        }
        Set<Long> value = new HashSet<>(Collections.singleton(unitKeywordTable.getUnitId()));
        handleBinLogEvent(DataTable.of(UnitKeywordIndex.class), unitKeywordTable.getKeyword(), value, type);
    }

    private static <K,V>  void handleBinLogEvent(IndexAware<K,V> index, K key, V value, OpType type) {
        switch (type) {
            case ADD:
                index.add(key, value);
                break;
            case UPDATE:
                index.update(key, value);
                break;
            case DELETE:
                index.delete(key, value);
                break;
                default:break;
        }
    }
}
