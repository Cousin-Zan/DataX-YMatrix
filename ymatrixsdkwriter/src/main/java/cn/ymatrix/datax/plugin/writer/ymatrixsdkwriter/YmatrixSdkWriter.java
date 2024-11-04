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
import java.util.List;
import java.util.Map;

public class YmatrixSdkWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }

            return writerSplitConfigs;
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private Configuration writerSliceConfig;
        private static final String CUSTOMER_LOG_TAG = "[>>>CUSTOMER<<<] ";
        private static MxBuilder mxBuilder;
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

//        private static int errCount = 0 ;

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

            initMxgateSDKBuilder(LOG);
            initMxgateClient(LOG);

        }

        @Override
        public void destroy() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {

            sendDataToMxgate(LOG, recordReceiver);

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
            } catch (IllegalStateException e){
                LOGGER.warn("############# MxBuilder Object has been created, Reuse MxBuilder Object! #############");
            } finally {
                mxBuilder = MxBuilder.instance();
            }
        }

        private void initMxgateClient(final Logger LOGGER) {

            try {
                if (requestType.equals("http")) {
                    client = mxBuilder.connect(httpHost, gRPCHost, schema, table);
                } else if (requestType.equals("grpc")) {
                    client = mxBuilder.connect(gRPCHost, gRPCHost, schema, table);
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


