package cn.ymatrix.datax.plugin.writer.ymatrixsdkwriter;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class YmatrixSdkWriter extends Writer {
    private static MxBuilder mxBuilder;
    private static AtomicInteger count = new AtomicInteger(0);
    private static Map<Integer, MxClient> mxClientMap = new ConcurrentHashMap<Integer, MxClient>();
    private static int groupSize = 5;
    private static int mxClientSize = 5;

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
        private static final String CUSTOMER_LOG_TAG = "[>>>CUSTOMER<<<] ";

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig;
        private int jobDestroyMS = 30000;

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
                this.jobDestroyMS = Integer.parseInt(this.writerSliceConfig.getString(Key.jobDestroyMS));
            } catch (Exception e) {
                e.printStackTrace();
                LOG.warn("parse jobDestroyMS({}) error: {},set to a default value: 30000", Key.groupSize, e.getMessage());
                this.jobDestroyMS = 30000;
            }
            LOG.info("init jobDestroyMS = {}", this.jobDestroyMS);

            try {
                groupSize = Integer.parseInt(this.writerSliceConfig.getString(Key.groupSize));
            } catch (Exception e) {
                e.printStackTrace();
                LOG.warn("parse groupSize({}) error: {},set to a fixed number: 5", Key.groupSize, e.getMessage());
                groupSize = 5;
            }
            LOG.info("init groupSize = {}", groupSize);

            try {
                mxClientSize = Integer.parseInt(this.writerSliceConfig.getString(Key.mxclientSize));
            } catch (Exception e) {
                e.printStackTrace();
                LOG.warn("parse mxClientSize({}) error: {},set to a fixed number: 5", Key.mxclientSize, e.getMessage());
                mxClientSize = 5;
            }
            LOG.info("init mxClientSize = {}", mxClientSize);

            initMxGateSDKBuilder(LOG);
            initMxGateClient(LOG);
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
        public void destroy() {
            LOG.info("begin to destroy the job {}", this.getClass().getSimpleName());
            // Before destroy
            for (MxClient mxClient : mxClientMap.values()) {
                // flush the data into the queue of the SDK
                mxClient.flush();
                LOG.info("mxClient({}) flush", mxClient.getClientName());
            }
            LOG.info("before destroy the job {}, ({})，wait for the SDK to flush data", this.getClass().getSimpleName(), this.jobDestroyMS);
            try {
                Thread.sleep(this.jobDestroyMS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            LOG.info("destroy the job {}", this.getClass().getSimpleName());
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

        private void initMxGateSDKBuilder(Logger LOGGER) {
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

        private void initMxGateClient(Logger LOGGER) {
            for (int i = 1; i <= mxClientSize; i++) {
                int groupNum = i % groupSize + 1;
                MxClient client = fetchMxGateClient(LOGGER, groupNum);
                if (client == null) {
                    LOGGER.error("fetch a nullable mxclient from the MxBuilder with groupNum={} and mxClientSize={}", groupNum, mxClientSize);
                    return;
                }
                mxClientMap.put(i, client);
                LOGGER.info("put mxclient({}->{}) into the map", i, client.getClientName());
            }
            LOGGER.info("total {} mxclients are in the map", mxClientMap.size());
        }

        private MxClient fetchMxGateClient(final Logger LOGGER, final int groupNum) {
            try {
                MxClient client;
                if (requestType.equals("http")) {
                    client = mxBuilder.connectWithGroup(httpHost, gRPCHost, schema, table, groupNum);
                } else {
                    client = mxBuilder.connectWithGroup(gRPCHost, gRPCHost, schema, table, groupNum);
                }
                if (compressWithZstd.equals("zstd")) {
                    LOGGER.info("with compress");
                    client.withCompress();
                    if (requestType.equals("grpc")) {
                        LOGGER.info("with base64");
                        client.withBase64Encode4Compress();
                    }
                }
                client.withEnoughLinesToFlush(Integer.parseInt(batchSize));
                return client;
            } catch (Exception e) {
                LOGGER.error("MxClient init error: {}" + e.getMessage());
                e.printStackTrace();
            }
            return null;
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration writerSliceConfig;
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
        private MxClient mxClient;
        private int taskNum;

        @Override
        public void init() {
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
            this.taskNum = count.incrementAndGet();
            int mxclientID = this.taskNum % mxClientSize + 1;
            LOG.info("begin to fetch mxclient with taskNum={}, mxClientID={}", taskNum, mxclientID);
            this.mxClient = mxClientMap.get(mxclientID);
            if (this.mxClient == null) {
                LOG.error("Get unexpected nullable MxClient from the mxClientMap");
                return;
            } else {
                LOG.info("fetch mxclient({}) with taskNum={}, mxClientID={} successfully", this.mxClient.getClientName(), taskNum, mxclientID);
            }
        }

        @Override
        public void destroy() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            LOG.info("YMatrix SDK writer task({}) before sendDataToMxGate", this.taskNum);
            sendDataToMxGate(LOG, recordReceiver);
            LOG.info("YMatrix SDK writer task({}) after sendDataToMxGate", this.taskNum);
        }

        private void sendDataToMxGate(Logger LOGGER, RecordReceiver recordReceiver) {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                this.mxClient.appendTuple(recordToString(record));
            }
        }

        private Tuple recordToString(Record record) {
            int recordLength = record.getColumnNumber();
            Column column;
            Tuple tuple = this.mxClient.generateEmptyTupleLite();
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