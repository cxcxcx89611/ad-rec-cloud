package com.imooc.ad.index.district;

import com.imooc.ad.index.IndexAware;
import com.imooc.ad.search.vo.feature.DistrictFeature;
import com.imooc.ad.utils.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

@Slf4j
@Component
public class UnitDistrictIndex implements IndexAware<String, Set<Long>> {

    private static Map<String, Set<Long>> districtUnitMap;

    private static Map<Long, Set<String>> unitDistrictMap;

    static {
        districtUnitMap = new ConcurrentHashMap<>();
        unitDistrictMap = new ConcurrentHashMap<>();
    }
    @Override
    public Set<Long> get(String key) {
        return districtUnitMap.get(key);
    }

    @Override
    public void add(String key, Set<Long> value) {
        log.info("UnitItIndex, before add: {}", unitDistrictMap);
        Set<Long> unitIds = CommonUtils.getorCreate(key, districtUnitMap, ConcurrentSkipListSet::new);
        unitIds.addAll(value);
        for(Long unitId : value) {
            Set<String> its = CommonUtils.getorCreate(unitId,unitDistrictMap,ConcurrentSkipListSet::new);
            its.add(key);
        }
        log.info("UnitItIndex, after add: {}", unitDistrictMap);
    }

    @Override
    public void update(String key, Set<Long> value) {
        log.error("district index can not support update");
    }

    @Override
    public void delete(String key, Set<Long> value) {
        Set<Long> unitIds = CommonUtils.getorCreate(key, districtUnitMap, ConcurrentSkipListSet::new);
        unitIds.removeAll(value);
        for(Long unitId : value) {
            Set<String> district = CommonUtils.getorCreate(unitId,unitDistrictMap,ConcurrentSkipListSet::new);
            district.remove(unitId);
        }
    }

    public boolean match(Long adUnitId, List<DistrictFeature.ProvinceAndCity> districts) {
        if(unitDistrictMap.containsKey(adUnitId) && CollectionUtils.isNotEmpty(unitDistrictMap.get(adUnitId))) {
            Set<String> unitDistricts = unitDistrictMap.get(adUnitId);

            List<String> targetUnitDistricts = districts.stream().map( d ->
                    CommonUtils.stringConcat(d.getProvince(),d.getProvince(), d.getCity())
            ).collect(Collectors.toList());
            return CollectionUtils.isSubCollection(targetUnitDistricts, unitDistricts);
        }
        return false;
    }

}
