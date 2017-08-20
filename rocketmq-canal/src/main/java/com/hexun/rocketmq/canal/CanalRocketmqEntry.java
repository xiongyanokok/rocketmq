package com.hexun.rocketmq.canal;

import java.util.ArrayList;
import java.util.List;

public class CanalRocketmqEntry {
    private long executeTime;
    private int entryType;
    private String logFileName;
    private long logFileOffset;
    private int eventType;
    private String schemaName;
    private String tableName;
    private List<List<CanalRocketmqDbColumn>> datas = new ArrayList<>();

    public List<List<CanalRocketmqDbColumn>> getDatas() {
        return datas;
    }

    public void addData(List<CanalRocketmqDbColumn> data) {
        this.datas.add(data);
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public void setEntryType(int entryType) {
        this.entryType = entryType;
    }

    public int getEntryType() {
        return entryType;
    }

    public void setLogFileName(String logFileName) {
        this.logFileName = logFileName;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public void setLogFileOffset(long logFileOffset) {
        this.logFileOffset = logFileOffset;
    }

    public long getLogFileOffset() {
        return logFileOffset;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public int getEventType() {
        return eventType;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
