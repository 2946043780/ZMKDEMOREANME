package com.reatime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package bean.UserInfoSup
 * @Author guo.jia.hui
 * @Date 2025/5/13 9:24
 * @description: UserInfoSup
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UserInfoSup {
    public Long uid;
    public String gender;
    public String height;
    public String unitHeight;
    public String weight;
    public String unitWeight;
    public Long tsMs;
}
