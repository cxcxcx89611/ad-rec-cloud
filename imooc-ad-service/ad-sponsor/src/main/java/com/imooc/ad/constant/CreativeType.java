package com.imooc.ad.constant;

import lombok.Getter;

@Getter
public enum CreativeType {

    IMAGE(1, "Picture"),
    VIDEO(2, "Video"),
    TXT(3, "txt");

    private int type;
    private String desc;

    CreativeType(int type, String desc) {
        this.type = type;
        this.desc = desc;
    }
}
