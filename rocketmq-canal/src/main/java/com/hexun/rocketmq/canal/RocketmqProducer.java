package com.hexun.rocketmq.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hexun.rocketmq.client.MessageProducer;
import org.apache.commons.lang.SystemUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.*;

public class RocketmqProducer {

    /**
     * logger
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 消息生产者
     */
    private MessageProducer producer;
    /**
     * 分隔符
     */
    private static final String SEP = SystemUtils.LINE_SEPARATOR;
    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static String contextFormat = null;
    private static String rowFormat = null;
    private static String transactionFormat = null;
    private static String topicPrefix = null;
    /**
     * 启用的数据库名字
     */
    private static HashSet<String> enabledDb = new HashSet<>();

    static {
        contextFormat = SEP + "****************************************************" + SEP;
        contextFormat += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        contextFormat += "* Start : [{}] " + SEP;
        contextFormat += "* End : [{}] " + SEP;
        contextFormat += "****************************************************" + SEP;

        rowFormat = SEP
                + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms"
                + SEP;

        transactionFormat = SEP + "================> binlog[{}:{}] , executeTime : {} , delay : {}ms" + SEP;
        String[] dbNames = ConfigUtils.getString("dbNames").split(",");
        enabledDb.addAll(Arrays.asList(dbNames));
        topicPrefix = ConfigUtils.getString("topicPrefix");
    }

    public RocketmqProducer(MessageProducer messageProducer) throws MQClientException {
        this.producer = messageProducer;
        this.producer.init();
    }

    /**
     * 发送 canal message 到消息队列
     *
     * @param message canal message
     * @return 发送结果
     */
    public boolean sendToMq(Message message) {
        long batchId = message.getId();
        int size = message.getEntries().size();
        if (batchId == -1 || size == 0) {
            return true;
        } else {
            printSummary(message, batchId, size);
            return sendToMq(message.getEntries());
        }
    }


    private void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (CanalEntry.Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }
        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }
        logger.info(contextFormat, batchId, size, memsize, format.format(new Date()), startPosition, endPosition);
    }

    private String buildPositionForDump(CanalEntry.Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":" + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
    }

    /**
     * Entry 发送到 rocketmq
     *
     * @param entrys List<CanalEntry.Entry>
     * @return 是否成功
     */
    private boolean sendToMq(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            //定义一个可序列化的推送给 rocketmq 的对象
            CanalRocketmqEntry canalRocketmqEntry = new CanalRocketmqEntry();
            long executeTime = entry.getHeader().getExecuteTime();
            // 变更数据的执行时间
            canalRocketmqEntry.setExecuteTime(executeTime);
            // 数据类型
            canalRocketmqEntry.setEntryType(entry.getEntryType().getNumber());
            // binlog 文件名
            canalRocketmqEntry.setLogFileName(entry.getHeader().getLogfileName());
            // binlog offset
            canalRocketmqEntry.setLogFileOffset(entry.getHeader().getLogfileOffset());

            long delayTime = new Date().getTime() - executeTime;

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                    CanalEntry.TransactionBegin begin;
                    try {
                        begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    logger.info(transactionFormat,
                            entry.getHeader().getLogfileName(),
                            entry.getHeader().getLogfileOffset(),
                            entry.getHeader().getExecuteTime(),
                            delayTime);
                    logger.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
                } else {
                    CanalEntry.TransactionEnd end;
                    try {
                        end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info("----------------\n END ----> transaction id: {}" + transactionFormat,
                            end.getTransactionId(),
                            entry.getHeader().getLogfileName(),
                            String.valueOf(entry.getHeader().getLogfileOffset()),
                            String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime));
                }
                continue;
            }

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChange;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    logger.error("parse event has an error , data:" + entry.toString(), e);
                    return false;
                }


                CanalEntry.EventType eventType = rowChange.getEventType();
                // ddl dml create index .....
                canalRocketmqEntry.setEventType(rowChange.getEventType().getNumber());
                // schema name 默认是数据库名
                canalRocketmqEntry.setSchemaName(entry.getHeader().getSchemaName());
                // table name
                canalRocketmqEntry.setTableName(entry.getHeader().getTableName());
                logger.info(rowFormat,
                        entry.getHeader().getLogfileName(),
                        entry.getHeader().getLogfileOffset(), entry.getHeader().getSchemaName(),
                        entry.getHeader().getTableName(), eventType,
                        entry.getHeader().getExecuteTime(), delayTime);

                if (eventType == CanalEntry.EventType.QUERY || rowChange.getIsDdl()) {
                    logger.info(" sql ----> " + rowChange.getSql() + SEP);
                    continue;
                }

                StringBuilder columnBuilder = new StringBuilder("\n");
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    List<CanalRocketmqDbColumn> dbColumns = printColumns(eventType, rowData, columnBuilder);
                    canalRocketmqEntry.addData(dbColumns);
                }
                logger.info(columnBuilder.toString());
                //current 数据库名字
                String dbName = entry.getHeader().getSchemaName();
                //current 表名
                String tableName = entry.getHeader().getTableName();
                //current position
                Long binlogPosition = entry.getHeader().getLogfileOffset();
                if (enabledDb.contains(dbName)) {
                    try {
                        SendResult sendResult = producer.sendOrderly(topicPrefix + dbName.toUpperCase(), "POS" + binlogPosition, canalRocketmqEntry, tableName);
                        if (sendResult == null || !SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                            return false;
                        }
                    } catch (Exception e) {
                        logger.error("CANAL发送到ROCKETMQ错误", e);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * print column
     *
     * @param builder StringBuilder
     */
    private List<CanalRocketmqDbColumn> printColumns(CanalEntry.EventType eventType, CanalEntry.RowData rowData, StringBuilder builder) {
        List<CanalRocketmqDbColumn> dbColumns = new ArrayList<>();
        if (eventType == CanalEntry.EventType.DELETE) {
            for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                dbColumns.add(printColumn(CanalEntry.EventType.DELETE, column, builder));
            }
        } else if (eventType == CanalEntry.EventType.INSERT) {
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                dbColumns.add(printColumn(CanalEntry.EventType.INSERT, column, builder));
            }
        } else if (eventType == CanalEntry.EventType.UPDATE) {
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                CanalRocketmqDbColumn dbColumn = printColumn(CanalEntry.EventType.UPDATE, column, builder);
                for (CanalEntry.Column before : rowData.getBeforeColumnsList()) {
                    if (before.getName().equals(dbColumn.getName())) {
                        dbColumn.setBeforeValue(before.getValue());
                    }
                }
                dbColumns.add(dbColumn);
            }
        }
        return dbColumns;
    }

    private CanalRocketmqDbColumn printColumn(CanalEntry.EventType eventType, CanalEntry.Column column, StringBuilder builder) {

        builder.append(column.getName()).append(" : ").append(column.getValue());
        builder.append("    type=").append(column.getMysqlType());
        if (column.getUpdated()) {
            builder.append("    update=").append(column.getUpdated());
        }
        builder.append(SEP);


        CanalRocketmqDbColumn dbColumn = new CanalRocketmqDbColumn();
        dbColumn.setName(column.getName());
        if (eventType == CanalEntry.EventType.INSERT) {
            dbColumn.setBeforeValue(null);
            dbColumn.setAfterValue(column.getValue());
        }
        if (eventType == CanalEntry.EventType.DELETE) {
            dbColumn.setBeforeValue(column.getValue());
            dbColumn.setAfterValue(null);
        }
        if (eventType == CanalEntry.EventType.UPDATE) {
            dbColumn.setBeforeValue(null);
            dbColumn.setAfterValue(column.getValue());
        }
        dbColumn.setType(column.getMysqlType());
        dbColumn.setUpdated(column.getUpdated());
        return dbColumn;
    }
}
