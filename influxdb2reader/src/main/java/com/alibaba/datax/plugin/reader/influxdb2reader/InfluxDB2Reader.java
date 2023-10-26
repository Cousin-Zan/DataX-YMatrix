package com.alibaba.datax.plugin.reader.influxdb2reader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.influxdb.impl.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author pengzan
 * @date 2023/10/25 17:08
 * @description
 */
public class InfluxDB2Reader extends Reader {

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;

        private Integer splitIntervalS;
        private long startDate;
        private long endDate;

        @Override
        public void init() {
            // 验证配置参数
            this.originalConfig = super.getPluginJobConf();
            originalConfig.getNecessaryValue(Key.ENDPOINT, InfluxDB2ReaderErrCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(Key.TOKEN, InfluxDB2ReaderErrCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(Key.ORG, InfluxDB2ReaderErrCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(Key.BUCKET, InfluxDB2ReaderErrCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(Key.MEASUREMENT, InfluxDB2ReaderErrCode.REQUIRED_VALUE);
            List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
            if (columns == null || columns.isEmpty()) {
                throw DataXException.asDataXException(
                        InfluxDB2ReaderErrCode.REQUIRED_VALUE, String.format("您提供配置文件有误，[%s]是必填参数，不允许为空或者留白 .", Key.COLUMN));
            }
            for (String specifyKey : Constant.MUST_CONTAINED_SPECIFY_KEYS) {
                if (!columns.contains(specifyKey)) {
                    throw DataXException.asDataXException(
                            InfluxDB2ReaderErrCode.ILLEGAL_VALUE, String.format("您提供配置文件有误，[%s]必须包含 '__time__'参数 .", Key.COLUMN));
                }
            }

            this.splitIntervalS = originalConfig.getInt(Key.INTERVAL_DATE_TIME, 60);
            if (splitIntervalS <= 0) {
                throw DataXException.asDataXException(
                        InfluxDB2ReaderErrCode.ILLEGAL_VALUE, String.format("您提供配置文件有误，[%s]必须大于0 .", Key.INTERVAL_DATE_TIME));
            }

            SimpleDateFormat format = new SimpleDateFormat(Constant.DEFAULT_DATA_FORMAT);
            String beginTime = originalConfig.getNecessaryValue(Key.BEGIN_DATE_TIME, InfluxDB2ReaderErrCode.REQUIRED_VALUE);
            try {
                this.startDate = format.parse(beginTime).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(
                        InfluxDB2ReaderErrCode.ILLEGAL_VALUE, String.format("您提供配置文件有误，[%s]参数必须按照[%s]格式填写.", Key.BEGIN_DATE_TIME, Constant.DEFAULT_DATA_FORMAT));
            }

            String endTime = originalConfig.getNecessaryValue(Key.END_DATE_TIME, InfluxDB2ReaderErrCode.REQUIRED_VALUE);
            try {
                this.endDate = format.parse(endTime).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(
                        InfluxDB2ReaderErrCode.ILLEGAL_VALUE, String.format("您提供配置文件有误，[%s]参数必须按照[%s]格式填写.", Key.END_DATE_TIME, Constant.DEFAULT_DATA_FORMAT));
            }

            if (startDate > endDate) {
                throw DataXException.asDataXException(
                        InfluxDB2ReaderErrCode.ILLEGAL_VALUE, String.format("您提供配置文件有误，[%s]必须大于[%s].", Key.END_DATE_TIME, Key.BEGIN_DATE_TIME));
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            ArrayList<Configuration> configurations = new ArrayList<>();

            // 按照 splitIntervalS 参数分割
            long start = this.startDate;
            long end = this.endDate;
            while (start < end) {
                Configuration clone = this.originalConfig.clone();
                clone.set(Key.BEGIN_DATE_TIME, start);
                start += splitIntervalS;
                if (start - 1 > end) {
                    clone.set(Key.END_DATE_TIME, end);
                } else {
                    clone.set(Key.END_DATE_TIME, start - 1);
                }
                configurations.add(clone);
                LOG.info("Configuration: {}", JSON.toJSONString(clone));
            }
            return configurations;
        }

//        @Override
//        public List<Configuration> split(int adviceNumber)
//        {
//            ArrayList<Configuration> configurations = new ArrayList<>();
//
//            // 按照 splitIntervalS 参数分割
//            long start = this.startDate;
//            long end = this.endDate;
//            Configuration clone = this.originalConfig.clone();
//            clone.set(Key.END_DATE_TIME, end);
//            clone.set(Key.END_DATE_TIME, start);
//            configurations.add(clone);
//            LOG.info("Configuration: {}", JSON.toJSONString(clone));
//            return configurations;
//        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(InfluxDB2Reader.Task.class);

        private Configuration originalConfig = null;
        private String endpoint;
        private String token;
        private String org;
        private String bucket;
        private String measurement;
        private long beginDate;

        private long endDate;

        private List<String> columns;

        private String queryQL;
        private InfluxDBClient influxDBClient;

        @Override
        public void init() {
            Configuration jobConf = this.getPluginJobConf();
            // 连接参数
            this.endpoint = jobConf.getString(Key.ENDPOINT);
            this.token = jobConf.getString(Key.TOKEN);
            this.org = jobConf.getString(Key.ORG);
            this.bucket = jobConf.getString(Key.BUCKET);
            this.measurement = jobConf.getString(Key.MEASUREMENT);

            //
            this.beginDate = jobConf.getLong(Key.BEGIN_DATE_TIME);
            this.endDate = jobConf.getLong(Key.END_DATE_TIME);

            //add

            this.columns = jobConf.getList(Key.COLUMN, String.class);

            this.queryQL = generalQueryQL();

            this.influxDBClient = InfluxDBClientFactory.create(endpoint, token.toCharArray(), org, bucket);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            QueryApi queryApi = this.influxDBClient.getQueryApi();
            List<FluxTable> tables;

            try {
                tables = queryApi.query(queryQL);
            } catch (com.influxdb.exceptions.UnauthorizedException e) {
                throw DataXException.asDataXException(InfluxDB2ReaderErrCode.UNAUTHORIZED_EXCEPTION,"Token认证失败");
            }

            if (tables.isEmpty()) {
                this.influxDBClient.close();
                LOG.info("SQL:{},数据为空", queryQL);
                return;
            }

            for (FluxTable fluxTable : tables) {
                List<FluxRecord> records = fluxTable.getRecords();
                for (FluxRecord fluxRecord : records) {
                    Record record = recordSender.createRecord();

                    for (String column : columns) {

                        Object value = fluxRecord.getValueByKey(column);

                        if (column.equals(Constant.TIME_SPECIFY_KEY)) {
//                            SimpleDateFormat format = new SimpleDateFormat(Constant.DEFAULT_DATA_FORMAT);
                            try {
                                assert value != null;
                                value = value.toString();
                            } catch (Exception e) {
                                throw DataXException.asDataXException(InfluxDB2ReaderErrCode.RUNTIME_EXCEPTION, "[time]:{} 转换失败" + value.toString());
                            }
                        }

                        if (value == null) {
                            record.addColumn(new StringColumn(null));
                        } else if (value instanceof Double) {
                            record.addColumn(new DoubleColumn((Double) value));
                        } else if (value instanceof Boolean) {
                            record.addColumn(new BoolColumn((Boolean) value));
                        } else if (value instanceof Date) {
                            record.addColumn(new DateColumn((Date) value));
                        } else if (value instanceof Integer) {
                            record.addColumn(new LongColumn((Integer) value));
                        } else if (value instanceof Long) {
                            record.addColumn(new LongColumn((Long) value));
                        } else if (value instanceof String) {
                            record.addColumn(new StringColumn((String) value));
                        } else {
                            throw DataXException.asDataXException(InfluxDB2ReaderErrCode.RUNTIME_EXCEPTION, "未知数据类型{} ," + value.toString());
                        }

                    }
                    recordSender.sendToWriter(record);
                }
            }
            influxDBClient.close();

        }


        private String generalQueryQL() {

            // 开始及结束日期转换
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            String startTime = format.format(new Date(beginDate));
            String endTime = format.format(new Date(endDate));

            StringBuilder queryBuilder = new StringBuilder();

            queryBuilder.append("from(bucket:\"").append(bucket).append("\")\n");

            queryBuilder.append("  |> range(");

            queryBuilder.append("start: ").append(startTime);
            queryBuilder.append(", stop: ").append(endTime);

            queryBuilder.append(") \n");

            queryBuilder.append("  |> filter(fn: (r) => ");
            queryBuilder.append(" r._measurement ==\"").append(measurement);


            queryBuilder.append("\") \n");

            if (! columns.isEmpty()) {
                queryBuilder.append("  |> filter(fn: (r) => r._field ==\"").append(columns.get(0)).append("\" ");
                if (columns.size() > 1) {
                    for(int i=1; i<columns.size();i++) {
                        queryBuilder.append(" or r._field == \"").append(columns.get(i)).append("\" ");
                    }
                }
                queryBuilder.append(") \n");
            }
            // convert fields to columns
            // refers https://docs.influxdata.com/influxdb/v2.0/query-data/flux/calculate-percentages/
            queryBuilder.append("  |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n");

            return queryBuilder.toString();
        }


        @Override
        public void destroy() {

        }
    }


}
