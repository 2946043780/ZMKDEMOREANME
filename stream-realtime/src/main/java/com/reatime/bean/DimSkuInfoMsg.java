package com.reatime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.label.domain.DimSkuInfoMsg
 * @Author guo.jia.hui
 * @Date 2025/5/15 09:10
 * @description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimSkuInfoMsg implements Serializable {
    private String skuid;
    private String spuid;
    private String c3id;
    private String tname;
}
