package com.imooc.ad.search.impl;

import com.alibaba.fastjson.JSON;
import com.imooc.ad.index.CommonStatus;
import com.imooc.ad.index.DataTable;
import com.imooc.ad.index.adunit.AdUnitIndex;
import com.imooc.ad.index.adunit.AdUnitObject;
import com.imooc.ad.index.creative.CreativeIndex;
import com.imooc.ad.index.creative.CreativeObject;
import com.imooc.ad.index.creativeunit.CreativeUnitIndex;
import com.imooc.ad.index.district.UnitDistrictIndex;
import com.imooc.ad.index.interest.UnitItIndex;
import com.imooc.ad.index.keyword.UnitKeywordIndex;
import com.imooc.ad.search.ISearch;
import com.imooc.ad.search.vo.SearchRequest;
import com.imooc.ad.search.vo.SearchResponse;
import com.imooc.ad.search.vo.feature.DistrictFeature;
import com.imooc.ad.search.vo.feature.FeatureRelation;
import com.imooc.ad.search.vo.feature.ItFeature;
import com.imooc.ad.search.vo.feature.KeywordFeature;
import com.imooc.ad.search.vo.media.AdSlot;
import com.netflix.hystrix.contrib.javanica.annotation.HystrixCommand;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import java.security.Key;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Created by Qinyi.
 */
@Slf4j
@Service
public class SearchImpl implements ISearch {

    public SearchResponse fallback(SearchRequest request, Throwable e) {
        return null;
    }

    @Override
    @HystrixCommand(fallbackMethod = "fallback")
    public SearchResponse fetchAds(SearchRequest request) {

        // 请求的广告位信息
        List<AdSlot> adSlots = request.getRequestInfo().getAdSlots();

        // 三个 Feature
        KeywordFeature keywordFeature =
                request.getFeatureInfo().getKeywordFeature();
        DistrictFeature districtFeature =
                request.getFeatureInfo().getDistrictFeature();
        ItFeature itFeature =
                request.getFeatureInfo().getItFeature();

        FeatureRelation relation = request.getFeatureInfo().getFeatureRelation();

        // 构造响应对象
        SearchResponse searchResponse = new SearchResponse();
        Map<String, List<SearchResponse.Creative>> adSlot2Ads = searchResponse.getAdSlot2Ads();

        for(AdSlot adSlot : adSlots) {
            Set<Long> targetUnitIdSet;

            Set<Long> adUnitIdSet = DataTable.of(AdUnitIndex.class).match(adSlot.getPositionType());

            if(relation == FeatureRelation.AND) {
                filterKeyWordFeature(adUnitIdSet, keywordFeature);
                filterDistrictFeature(adUnitIdSet, districtFeature);
                filterItTagFeature(adUnitIdSet, itFeature);
                targetUnitIdSet = adUnitIdSet;
            } else {
                targetUnitIdSet = getORRelationUnitIds(adUnitIdSet, keywordFeature, districtFeature, itFeature);
            }

            List<AdUnitObject> unitObjects = DataTable.of(AdUnitIndex.class).fetch(targetUnitIdSet);

            filterAdUnitAndPlanStatus(unitObjects,CommonStatus.VALID);

            List<Long> adIds = DataTable.of(CreativeUnitIndex.class).selectAds(unitObjects);

            List<CreativeObject> creativeObjects = DataTable.of(CreativeIndex.class).fetch(adIds);

            filterCreativeByAdSlot(creativeObjects, adSlot.getWidth(), adSlot.getHeight(), adSlot.getType());

            adSlot2Ads.put(adSlot.getAdSlotCode(), buildCreativeResponse(creativeObjects));
        }
        log.info("fetch ads: {} --- {]", JSON.toJSONString(request), JSON.toJSONString(searchResponse));
        return searchResponse;
    }

    private Set<Long> getORRelationUnitIds(Set<Long> adUnitIdSet,
                                           KeywordFeature keywordFeature,
                                           DistrictFeature districtFeature,
                                           ItFeature itFeature) {
        if(CollectionUtils.isEmpty(adUnitIdSet)){
            return Collections.EMPTY_SET;
        }
        Set<Long> keywordUnitIdSet = new HashSet<>(adUnitIdSet);
        Set<Long> districtUnitIdSet = new HashSet<>(adUnitIdSet);
        Set<Long> itUnitIdSet = new HashSet<>(adUnitIdSet);

        filterItTagFeature(itUnitIdSet, itFeature);
        filterDistrictFeature(districtUnitIdSet, districtFeature);
        filterKeyWordFeature(keywordUnitIdSet, keywordFeature);

        return new HashSet<>(CollectionUtils.union(CollectionUtils.union(keywordUnitIdSet,districtUnitIdSet),itUnitIdSet));
    }
    private void filterKeyWordFeature(Collection<Long> adUnitIds, KeywordFeature keywordFeature) {
        if(CollectionUtils.isEmpty(adUnitIds)){
            return;
        }
        if(CollectionUtils.isNotEmpty(keywordFeature.getKeywords())){
            CollectionUtils.filter(adUnitIds,
                    adUnitId -> DataTable.of(UnitKeywordIndex.class)
                            .match(adUnitId, keywordFeature.getKeywords()));
        }
    }

    private void filterDistrictFeature(Collection<Long> adUnitIds, DistrictFeature districtFeature) {
        if(CollectionUtils.isEmpty(adUnitIds)){
            return;
        }
        if(CollectionUtils.isNotEmpty(districtFeature.getDistricts())){
            CollectionUtils.filter(adUnitIds,
                    adUnitId -> DataTable.of(UnitDistrictIndex.class)
                            .match(adUnitId, districtFeature.getDistricts()));
        }
    }

    private void filterItTagFeature(Collection<Long> adUnitIds, ItFeature itFeature) {
        if(CollectionUtils.isEmpty(adUnitIds)){
            return;
        }
        if(CollectionUtils.isNotEmpty(itFeature.getIts())){
            CollectionUtils.filter(adUnitIds,
                    adUnitId -> DataTable.of(UnitItIndex.class)
                            .match(adUnitId, itFeature.getIts()));
        }
    }

    private void filterAdUnitAndPlanStatus(List<AdUnitObject> unitObjects, CommonStatus status) {
        if(CollectionUtils.isEmpty(unitObjects)) {
            return;
        }
        CollectionUtils.filter(unitObjects, object -> object.getUnitStatus().equals(status.getStatus())
                && object.getAdPlanObject().getPlanStatus().equals(status.getStatus()));
    }

    private  void filterCreativeByAdSlot(List<CreativeObject> creativeObjects,
                                         Integer width, Integer height, List<Integer> type) {
        if(CollectionUtils.isEmpty(creativeObjects)) {
            return;
        }
        CollectionUtils.filter(creativeObjects, creativeObject ->
                creativeObject.getAuditStatus().equals(CommonStatus.VALID.getStatus())
                && creativeObject.getWidth().equals(width)
                && creativeObject.getHeight().equals(height)
                && type.contains(creativeObject.getType()));
    }

    private List<SearchResponse.Creative> buildCreativeResponse(List<CreativeObject> creatives) {
        if(CollectionUtils.isEmpty(creatives)) {
            return Collections.EMPTY_LIST;
        }
        CreativeObject randomObject = creatives.get(Math.abs(new Random().nextInt())%creatives.size());
        return Collections.singletonList(SearchResponse.convert(randomObject));
    }
}
