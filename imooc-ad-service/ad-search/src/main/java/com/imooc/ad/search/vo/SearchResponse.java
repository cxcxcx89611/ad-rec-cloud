package com.imooc.ad.search.vo;

import com.imooc.ad.index.creative.CreativeObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchResponse {

    public Map<String, List<Creative>> adSlot2Ads = new HashMap<>();

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Creative {
        private Long adId;
        private String adUrl;
        private Integer width;
        private  Integer height;
        private  Integer type;
        private Integer materialType;

        private List<String> showMonitorUrl = Arrays.asList("www.imooc.com", "www.imoooc.com");
        private List<String> clickMonitorUrl = Arrays.asList("www.imooc.com", "www.imoooc.com");
    }
    public static Creative convert(CreativeObject object) {
        Creative creative = new Creative();
        creative.setAdId(object.getAdId());
        creative.setAdUrl(object.getAdUrl());
        creative.setWidth(object.getWidth());
        creative.setHeight(object.getHeight());
        creative.setMaterialType(object.getMaterialType());
        return creative;
    }


}
