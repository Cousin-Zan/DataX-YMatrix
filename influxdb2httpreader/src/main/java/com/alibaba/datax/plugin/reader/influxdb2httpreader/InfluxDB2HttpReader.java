package com.alibaba.datax.plugin.reader.influxdb2httpreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author pengzan
 * @date 2023/10/25 17:08
 * @description
 */
public class InfluxDB2HttpReader extends Reader {

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
            originalConfig.getNecessaryValue(Key.ENDPOINT, InfluxDB2HttpReaderErrCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(Key.TOKEN, InfluxDB2HttpReaderErrCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(Key.ORG, InfluxDB2HttpReaderErrCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(Key.BUCKET, InfluxDB2HttpReaderErrCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(Key.MEASUREMENT, InfluxDB2HttpReaderErrCode.REQUIRED_VALUE);
            List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
            if (columns == null || columns.isEmpty()) {
                throw DataXException.asDataXException(
                        InfluxDB2HttpReaderErrCode.REQUIRED_VALUE, String.format("您提供配置文件有误，[%s]是必填参数，不允许为空或者留白 .", Key.COLUMN));
            }
            for (String specifyKey : Constant.MUST_CONTAINED_SPECIFY_KEYS) {
                if (!columns.contains(specifyKey)) {
                    throw DataXException.asDataXException(
                            InfluxDB2HttpReaderErrCode.ILLEGAL_VALUE, String.format("您提供配置文件有误，[%s]必须包含 '__time__'参数 .", Key.COLUMN));
                }
            }

            this.splitIntervalS = originalConfig.getInt(Key.INTERVAL_DATE_TIME, 60);
            if (splitIntervalS <= 0) {
                throw DataXException.asDataXException(
                        InfluxDB2HttpReaderErrCode.ILLEGAL_VALUE, String.format("您提供配置文件有误，[%s]必须大于0 .", Key.INTERVAL_DATE_TIME));
            }

            SimpleDateFormat format = new SimpleDateFormat(Constant.DEFAULT_DATA_FORMAT);
            String beginTime = originalConfig.getNecessaryValue(Key.BEGIN_DATE_TIME, InfluxDB2HttpReaderErrCode.REQUIRED_VALUE);
            try {
                this.startDate = format.parse(beginTime).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(
                        InfluxDB2HttpReaderErrCode.ILLEGAL_VALUE, String.format("您提供配置文件有误，[%s]参数必须按照[%s]格式填写.", Key.BEGIN_DATE_TIME, Constant.DEFAULT_DATA_FORMAT));
            }

            String endTime = originalConfig.getNecessaryValue(Key.END_DATE_TIME, InfluxDB2HttpReaderErrCode.REQUIRED_VALUE);
            try {
                this.endDate = format.parse(endTime).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(
                        InfluxDB2HttpReaderErrCode.ILLEGAL_VALUE, String.format("您提供配置文件有误，[%s]参数必须按照[%s]格式填写.", Key.END_DATE_TIME, Constant.DEFAULT_DATA_FORMAT));
            }

            if (startDate > endDate) {
                throw DataXException.asDataXException(
                        InfluxDB2HttpReaderErrCode.ILLEGAL_VALUE, String.format("您提供配置文件有误，[%s]必须大于[%s].", Key.END_DATE_TIME, Key.BEGIN_DATE_TIME));
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

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(InfluxDB2HttpReader.Task.class);

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

        }

        @Override
        public void startRead(RecordSender recordSender) {


            try {
                Response response = queryBySql(endpoint, token, org, bucket, queryQL);
                if (response.isSuccessful()) {
                    assert response.body() != null;
                    BufferedReader responseBody = new BufferedReader(response.body().charStream());
                    String line;

                    responseBody.readLine();
                    while ((line = responseBody.readLine()) != null) {
                        if (line.equals("")) {
                            continue;
                        }
                        Record record = recordSender.createRecord();

                        String[] sourceLine = StringUtils.split(line, ',');

                        for (int i = 4; i < sourceLine.length; i++) {

                            if (i == 5) {
                                continue;
                            }
                            String value = sourceLine[i];

                            if (value == null | Objects.equals(value, "")) {
                                record.addColumn(new StringColumn(null));

                            } else {
                                record.addColumn(new StringColumn(value));
                            }
                        }
                        recordSender.sendToWriter(record);
                    }
                } else {
                    System.out.println("Request failed with code: " + response.code());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


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

            if (!columns.isEmpty()) {
                queryBuilder.append("  |> filter(fn: (r) => r._field ==\"").append(columns.get(0)).append("\" ");
                if (columns.size() > 1) {
                    for (int i = 1; i < columns.size(); i++) {
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

        public Response queryBySql(String endpoint, String token, String org, String bucket, String queryQL) throws IOException {
            OkHttpClient client = new OkHttpClient();

            String url = String.format("%s/api/v2/query?org=%s", endpoint, org);

            MediaType mediaType = MediaType.parse("application/vnd.flux");
            RequestBody body = RequestBody.create(mediaType, queryQL);

            Request request = new Request.Builder()
                    .url(url)
                    .addHeader("Authorization", "Token " + token)
                    .addHeader("Content-Type", "application/vnd.flux")
                    .addHeader("Accept", "application/csv")
                    .post(body)
                    .build();

            Response response = client.newCall(request).execute();
            return response;
        }

        @Override
        public void destroy() {

        }
    }


}
