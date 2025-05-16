package com.reatime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package bean.UserInfo
 * @Author guo.jia.hui
 * @Date 2025/5/13 9:24
 * @description: userinfo
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UserInfo {
    public Long id;
    public String loginName;
    public String name;
    public String phone;
    public String email;
    public Long birthday;
    public String gender;
    public Long tsMs;
}
