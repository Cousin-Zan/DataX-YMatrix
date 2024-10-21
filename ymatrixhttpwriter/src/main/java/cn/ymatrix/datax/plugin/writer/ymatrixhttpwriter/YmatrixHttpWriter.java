package cn.ymatrix.datax.plugin.writer.ymatrixhttpwriter;

import cn.ymatrix.logger.LoggerLevel;
import cn.ymatrix.logger.MxLogger;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class YmatrixHttpWriter extends Writer {
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

        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");

        private Configuration writerSliceConfig;

        private String fieldDelimiter;

        private String batchsize;

        private String tablename;

        private String url;

        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();

            this.fieldDelimiter = this.writerSliceConfig.getString(Key.FIELD_DELIMITER);
            this.batchsize = this.writerSliceConfig.getString(Key.BATCH_SIZE);
            this.tablename = this.writerSliceConfig.getString(Key.TABLENAME);
            this.url = this.writerSliceConfig.getString(Key.URL);
        }

        @Override
        public void destroy() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            MxLogger.loggerLevel(LoggerLevel.INFO);
            Logger LOGGER = MxLogger.init(Task.class);

            Record record;
            int receivedCount = 0;
            int blank = 0;

            StringBuilder sb = new StringBuilder(tablename + "\n");

            while ((record = recordReceiver.getFromReader()) != null) {

                blank = 1;

                sb.append(recordToString(record));

                receivedCount++;

                if (receivedCount % Integer.parseInt(batchsize) == 0) {

                    sb.deleteCharAt(sb.length() - 1);
                    try {
                        HttpURLRequest(LOGGER, sb.toString(), url);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    sb = new StringBuilder(tablename + "\n");

                    blank = 0;
                }
            }

            sb.deleteCharAt(sb.length() - 1);
            if (blank == 1) {
                try {
                    HttpURLRequest(LOGGER, sb.toString(), url);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void HttpURLRequest(
                Logger LOGGER, String postData, String url)
                throws Exception {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "text/plain");

            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.write(postData.getBytes(StandardCharsets.UTF_8));
            wr.flush();
            wr.close();

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;

            while (((inputLine = in.readLine()) != null)) {

                // 检查行是否包含错误
                if (inputLine.contains("line")) {
                    // 如果包含错误，输出错误行

                    String errInfo = in.readLine();
                    int position = Integer.parseInt((inputLine.split(": ")[1]));
                    String errData = postData.split("\n")[position - 1];

                    LOGGER.info("Error " + inputLine + ": " + errInfo + ": " + errData);

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
                    super.getTaskPluginCollector().collectDirtyRecord(record, "Error " + inputLine + ": " + errInfo);
                }
            }

            in.close();

            // 如果一批数据中有部分成功返回部分的报错信息
            // 如果一批数据全部出错，显示一批的报错信息
            if (500 == con.getResponseCode()) {
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(con.getErrorStream(), StandardCharsets.UTF_8));
                String line = null;
                String result = "";
                while ((line = reader.readLine()) != null) {
                    result += line;
                }
                reader.close();
                LOGGER.info("errorMsg: " + result);
            }
        }


        private String recordToString(Record record) {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return NEWLINE_FLAG;
            }

            Column column;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);

                if (column.asString() == null) {
                    sb.append(fieldDelimiter);
                } else {

                    if (column.getType().toString() == "STRING") {
                        sb.append('"' + column.asString() + '"').append(fieldDelimiter);
                    } else {
                        sb.append(column.asString()).append(fieldDelimiter);
                    }
                }
            }
            sb.setLength(sb.length() - 1);
            sb.append(NEWLINE_FLAG);

            return sb.toString();
        }

    }

}
