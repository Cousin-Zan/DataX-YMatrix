package cn.ymatrix.datax.plugin.writer.ymatrixsdkwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import org.apache.commons.codec.binary.Hex;

import cn.ymatrix.apiclient.DataPostListener;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.apiclient.Result;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.RequestType;
import cn.ymatrix.data.Tuple;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class YmatrixSdkWriter extends Writer {

    private static MxBuilder mxBuilder;
    private static int groupSize = 10;
    private static AtomicInteger count = new AtomicInteger(0);

    public static class Job extends Writer.Job {
        private String cacheCapacity;
        private String cacheEnqueueTimeout;
        private String sdkConcurrency;
        private String requestTimeoutMillis;
        private String maxRequestQueued;
        private String maxRetryAttempts;
        private String retryWaitDurationMillis;
        private String batchSize;
        private String requestType;
        private String dropAll;
        private String asyncMode;
        private String httpHost;
        private String gRPCHost;
        private String schema;
        private String table;
        private String compressWithZstd;
        private Configuration writerSliceConfig;

        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.writerSliceConfig = getPluginJobConf();
            this.cacheCapacity = this.writerSliceConfig.getString(Key.cacheCapacity);
            this.cacheEnqueueTimeout = this.writerSliceConfig.getString(Key.cacheEnqueueTimeout);
            this.sdkConcurrency = this.writerSliceConfig.getString(Key.sdkConcurrency);
            this.requestTimeoutMillis = this.writerSliceConfig.getString(Key.requestTimeoutMillis);
            this.maxRequestQueued = this.writerSliceConfig.getString(Key.maxRequestQueued);
            this.maxRetryAttempts = this.writerSliceConfig.getString(Key.maxRetryAttempts);
            this.retryWaitDurationMillis = this.writerSliceConfig.getString(Key.retryWaitDurationMillis);
            this.batchSize = this.writerSliceConfig.getString(Key.batchSize);
            this.requestType = this.writerSliceConfig.getString(Key.requestType);
            this.dropAll = this.writerSliceConfig.getString(Key.dropAll);
            this.asyncMode = this.writerSliceConfig.getString(Key.asyncMode);
            this.httpHost = this.writerSliceConfig.getString(Key.httpHost);
            this.gRPCHost = this.writerSliceConfig.getString(Key.gRPCHost);
            this.schema = this.writerSliceConfig.getString(Key.schema);
            this.table = this.writerSliceConfig.getString(Key.table);
            this.compressWithZstd = this.writerSliceConfig.getString(Key.compressWithZstd);
            try {
                groupSize = Integer.parseInt(this.writerSliceConfig.getString(Key.groupSize));
            } catch (Exception e) {
                LOG.warn("parse groupSize error: {}", e.getMessage());
            } finally {
                groupSize = 10;
            }
            LOG.info("config group size: {}", groupSize);
            initMxgateSDKBuilder(LOG);
        }

        @Override
        public void prepare() {

            String username = originalConfig.getString(Key.userName);
            String password = originalConfig.getString(Key.passWord);

            String jdbcUrl = originalConfig.getString(Key.jdbcUrl);
            String preSql = originalConfig.getString(Key.preSql);

            if (preSql != null) {

                Connection conn = null;
                Statement stmt = null;

                LOG.info("开始执行 preSQL: {}... context info:{}.", preSql, jdbcUrl);

                try {
                    Class.forName("org.postgresql.Driver");
                    conn = DriverManager.getConnection(jdbcUrl, username, password);
                    stmt = conn.createStatement();
                    stmt.executeUpdate(preSql);

                    LOG.info("执行 preSQL: {} 完毕!  context info:{}.", preSql, jdbcUrl);
                } catch (Exception e) {
                    LOG.error("执行 preSQL: {} 失败! 请检查参数设置!  context info:{}.", preSql, jdbcUrl);
                    LOG.error("执行 preSQL: {} 失败原因：{}", preSql, e.getMessage());

                    throw new RuntimeException(e);
                }

                if (null != stmt) {
                    try {
                        stmt.close();
                    } catch (SQLException unused) {
                    }
                }
                if (null != conn) {
                    try {
                        conn.close();
                    } catch (SQLException unused) {
                    }
                }
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration cfg = this.originalConfig.clone();
                writerSplitConfigs.add(this.originalConfig);
            }
            return writerSplitConfigs;
        }

        @Override
        public void destroy() {

        }

        private void initMxgateSDKBuilder(Logger LOGGER) {
            MxBuilder.Builder builder =
                    MxBuilder.newBuilder()
                            .withDropAll(false)
                            .withCacheCapacity(Integer.parseInt(cacheCapacity))
                            .withCacheEnqueueTimeout(Long.parseLong(cacheEnqueueTimeout))
                            .withConcurrency(Integer.parseInt(sdkConcurrency))
                            .withRequestTimeoutMillis(Integer.parseInt(requestTimeoutMillis))
                            .withMaxRequestQueued(Integer.parseInt(maxRequestQueued))
                            .withMaxRetryAttempts(Integer.parseInt(maxRetryAttempts))
                            .withRetryWaitDurationMillis(Integer.parseInt(retryWaitDurationMillis));

            if (requestType.equals("http")) {
                builder.withRequestType(RequestType.WithHTTP);
            } else if (requestType.equals("grpc")) {
                builder.withRequestType(RequestType.WithGRPC);
            } else {
                LOGGER.error("Invalid request type, should be http or grpc");
                return;
            }
            if (dropAll.equals("dropAll")) {
                builder.withDropAll(true);
            }
            if (asyncMode.equals("async")) {
                builder.withRequestAsync(true);
            }

            try {
                mxBuilder = builder.build();
                LOGGER.info("############# Build mxBuilder for job {} #############", this.getClass().getName());
            } catch (IllegalStateException e){
                LOGGER.warn("############# MxBuilder Object has been created, Reuse MxBuilder Object! #############");
            } finally {
                mxBuilder = MxBuilder.instance();
            }
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private Configuration writerSliceConfig;
        private static final String CUSTOMER_LOG_TAG = "[>>>CUSTOMER<<<] ";
        private MxClient client;
        private String cacheCapacity;
        private String cacheEnqueueTimeout;
        private String sdkConcurrency;
        private String requestTimeoutMillis;
        private String maxRequestQueued;
        private String maxRetryAttempts;
        private String retryWaitDurationMillis;
        private String batchSize;
        private String requestType;
        private String dropAll;
        private String asyncMode;
        private String httpHost;
        private String gRPCHost;
        private String schema;
        private String table;
        private String compressWithZstd;

        private int groupNum;

        @Override
        public void init() {this.writerSliceConfig = getPluginJobConf();
            this.cacheCapacity = this.writerSliceConfig.getString(Key.cacheCapacity);
            this.cacheEnqueueTimeout = this.writerSliceConfig.getString(Key.cacheEnqueueTimeout);
            this.sdkConcurrency = this.writerSliceConfig.getString(Key.sdkConcurrency);
            this.requestTimeoutMillis = this.writerSliceConfig.getString(Key.requestTimeoutMillis);
            this.maxRequestQueued = this.writerSliceConfig.getString(Key.maxRequestQueued);
            this.maxRetryAttempts = this.writerSliceConfig.getString(Key.maxRetryAttempts);
            this.retryWaitDurationMillis = this.writerSliceConfig.getString(Key.retryWaitDurationMillis);
            this.batchSize = this.writerSliceConfig.getString(Key.batchSize);
            this.requestType = this.writerSliceConfig.getString(Key.requestType);
            this.dropAll = this.writerSliceConfig.getString(Key.dropAll);
            this.asyncMode = this.writerSliceConfig.getString(Key.asyncMode);
            this.httpHost = this.writerSliceConfig.getString(Key.httpHost);
            this.gRPCHost = this.writerSliceConfig.getString(Key.gRPCHost);
            this.schema = this.writerSliceConfig.getString(Key.schema);
            this.table = this.writerSliceConfig.getString(Key.table);
            this.compressWithZstd = this.writerSliceConfig.getString(Key.compressWithZstd);

            this.groupNum = count.getAndIncrement() % groupSize + 1;
            LOG.info("config group num={} for task: {}", groupNum, this.getClass().getName());
            initMxgateClient(LOG);
        }

        @Override
        public void destroy() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            sendDataToMxgate(LOG, recordReceiver);
        }



        private void initMxgateClient(final Logger LOGGER) {
            try {
                if (requestType.equals("http")) {
//                    client = mxBuilder.connect(httpHost, gRPCHost, schema, table);
                    client = mxBuilder.connectWithGroup(httpHost, gRPCHost, schema, table, this.groupNum);
                } else if (requestType.equals("grpc")) {
//                    client = mxBuilder.connect(gRPCHost, gRPCHost, schema, table);
                    client = mxBuilder.connectWithGroup(gRPCHost, gRPCHost, schema, table, this.groupNum);
                }
            } catch (Exception e) {
                LOGGER.error("MxClient init error: {}" + e.getMessage());
                e.printStackTrace();
            }

            if (compressWithZstd.equals("zstd")) {
                LOGGER.info("with compress");
                client.withCompress();
                if (requestType.equals("grpc")) {
                    LOGGER.info("with base64");
                    client.withBase64Encode4Compress();
                }
            }

            client.withIntervalToFlushMillis(2000);
            client.withEnoughLinesToFlush(Integer.parseInt(batchSize));

            client.registerDataPostListener(
                    new DataPostListener() {
                        public void onSuccess(Result result) {
                            LOGGER.info(CUSTOMER_LOG_TAG + "Send tuples success: " + result.getMsg());
                            LOGGER.info(CUSTOMER_LOG_TAG + "Succeed lines onSuccess callback " + result.getSucceedLines());
                        }

                        public void onFailure(Result result) {
                            LOGGER.error(
                                    CUSTOMER_LOG_TAG
                                            + "Sendtuples fail error tuples:{} "
                                            + result.getErrorTuplesMap());
                            Record record = new Record() {
                                @Override
                                public void addColumn(Column column) {}
                                @Override
                                public void setColumn(int i, Column column) {}
                                @Override
                                public Column getColumn(int i) {
                                    return null;
                                }
                                @Override
                                public int getColumnNumber() {
                                    return 0;
                                }
                                @Override
                                public int getByteSize() {
                                    return 0;
                                }
                                @Override
                                public int getMemorySize() {
                                    return 0;
                                }
                            };

                            for (Map.Entry<Tuple, String> entry : result.getErrorTuplesMap().entrySet()) {
                                LOGGER.error(
                                        CUSTOMER_LOG_TAG
                                                + "error tuple of table="
                                                + entry.getKey().getTableName()
                                                + " tuple="
                                                + entry.getKey()
                                                + " reason="
                                                + entry.getValue());

//                                errCount++;
//                                LOGGER.info("*****************"+errCount);


                                Task.super.getTaskPluginCollector().collectDirtyRecord(record,
                                        "error tuple of table="
                                                + entry.getKey().getTableName()
                                                + " tuple="
                                                + entry.getKey()
                                                + " reason="
                                                + entry.getValue());
                            }
                        }
                    });
        }

        private void sendDataToMxgate(Logger LOGGER, RecordReceiver recordReceiver) {

            Record record;

            while ((record = recordReceiver.getFromReader()) != null) {

                client.appendTuple(recordToString(client, record));

            }
        }

        private Tuple recordToString(MxClient client, Record record) {
            int recordLength = record.getColumnNumber();

            Column column;
            Tuple tuple = client.generateEmptyTupleLite();

            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);

                if (column.getType().toString() == "BYTES") {
                    byte[] rawData = column.asBytes();
                    String hexString = Hex.encodeHexString(rawData);
                    tuple.addColumn(String.valueOf(i), (column.asBytes() == null) ? "" : "\\x"+hexString);
                } else {
                    tuple.addColumn(String.valueOf(i), (column.asString() == null) ? "" : column.asString());
                }

            }
            return tuple;
        }
    }
}



