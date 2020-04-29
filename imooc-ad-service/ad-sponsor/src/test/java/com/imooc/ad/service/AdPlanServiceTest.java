package com.imooc.ad.service;

import com.imooc.ad.Application;
import com.imooc.ad.exception.AdException;
import com.imooc.ad.vo.AdPlanGetRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.CollectionUtils;

import java.util.Collections;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {Application.class}, webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class AdPlanServiceTest {

    @Autowired
    private IAdPlanService adPlanService;

    @Test
    public void testGetAdPlan() throws AdException {
        System.out.println(adPlanService.getAdPlanByIds(new AdPlanGetRequest(15L, Collections.singletonList(10L))));
    }
}
