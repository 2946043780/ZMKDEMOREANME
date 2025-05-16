package com.reatime.util;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @Package utils.ConfigUtils2
 * @Author guo.jia.hui
 * @Date 2025/5/14 15:23
 * @description:
 */
public final class ConfigUtils2 {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    private static Properties properties;

    static {
        try {
            properties = new Properties();
            properties.load(ConfigUtils.class.getClassLoader().getResourceAsStream("common-config.properties"));
        } catch (IOException e) {
            logger.error("加载配置文件出错, exit 1", e);
            System.exit(1);
        }
    }

    public static String getString(String key) {
        // logger.info("加载配置[" + key + "]:" + value);
        return properties.getProperty(key).trim();
    }

    public static int getInt(String key) {
        String value = properties.getProperty(key).trim();
        return Integer.parseInt(value);
    }

    public static int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key).trim();
        return Strings.isNullOrEmpty(value) ? defaultValue : Integer.parseInt(value);
    }

    public static long getLong(String key) {
        String value = properties.getProperty(key).trim();
        return Long.parseLong(value);
    }

    public static long getLong(String key, long defaultValue) {
        String value = properties.getProperty(key).trim();
        return Strings.isNullOrEmpty(value) ? defaultValue : Long.parseLong(value);
    }
}
