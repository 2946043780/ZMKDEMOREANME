package com.reatime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.stream.domain.DimBaseCategory
 * @Author guo.jia.hui
 * @Date 2025/5/14 08:40
 * @description: base category all data
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory implements Serializable {

    private String id;
    private String b3name;
    private String b2name;
    private String b1name;


}
