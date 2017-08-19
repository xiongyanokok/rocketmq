package com.hexun.rocketmq.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.LayoutBase;
import com.hexun.common.utils.JsonUtils;

/**
 * JacksonLayout
 */
public class JacksonLayout extends LayoutBase<ILoggingEvent> {

    @Override
    public String doLayout(ILoggingEvent event) {
        return JsonUtils.obj2String(event);
    }
}
