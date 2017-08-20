package com.hexun.rocketmq.canal;

import com.hexun.common.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class ConfigUtils {
    /**
     * logger
     */
    private static Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    /**
     * default config file name is canal
     */
    private static ResourceBundle resourceBundle = ResourceBundle.getBundle("canal");

    /**
     * 根据 key 获取 string 配置节
     *
     * @param key String
     * @return String
     */
    public static String getString(String key) {
        if (StringUtils.isBlank(key)) {
            return "";
        }
        try {
            return resourceBundle.getString(key);
        } catch (MissingResourceException e) {
            logger.error("错误信息:配置文件canal未找到key值为" + key + "的键。");
        }
        return "";
    }

}