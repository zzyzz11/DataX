package com.alibaba.datax.plugin.reader.dwmetareader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

public class DwmetaReader extends Reader {

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;
        private IAcsClient client=null;
        private List<String> tableGuids=new ArrayList<String>();;

        // dw链接参数
        private String regionid;
        private String accessKeyId;
        private String accessSecret;
        private String product;
        private String endpoint;
        private String appguids;


        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            this.regionid = this.originConfig.getNecessaryValue(Key.REGIONID, DwmetaReaderErrorCode.REQUIRED_VALUE);
            this.accessKeyId = this.originConfig.getNecessaryValue(Key.ACCESSKEYID, DwmetaReaderErrorCode.REQUIRED_VALUE);
            this.accessSecret = this.originConfig.getNecessaryValue(Key.ACCESSSECRET, DwmetaReaderErrorCode.REQUIRED_VALUE);
            this.product = this.originConfig.getNecessaryValue(Key.PRODUCT, DwmetaReaderErrorCode.REQUIRED_VALUE);
            this.endpoint = this.originConfig.getNecessaryValue(Key.ENDPOINT, DwmetaReaderErrorCode.REQUIRED_VALUE);
            this.appguids = this.originConfig.getNecessaryValue(Key.APPGUIDS, DwmetaReaderErrorCode.REQUIRED_VALUE);
            IClientProfile profile = DefaultProfile.getProfile(this.regionid, this.accessKeyId, this.accessSecret);
            DefaultProfile.addEndpoint(this.regionid,this.product, this.endpoint);
            this.client = new DefaultAcsClient(profile);
        }

        @Override
        public void prepare() {
            LOG.debug("prepare() begin...");

            this.tableGuids = DwmetaInfo.getAllTables(this.client,this.appguids);

            LOG.info(String.format("您即将读取的表数为: [%s]", this.tableGuids.size()));
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

            int splitNumber = this.tableGuids.size();
            if (0 == splitNumber) {
                throw DataXException.asDataXException(DwmetaReaderErrorCode.EMPTY_PROJECT_EXCEPTION,
                        String.format("未能找到待读取的表,请确认您的配置项appguids: %s", this.originConfig.getString(Key.APPGUIDS)));
            }

            List<List<String>> splitedSourceFiles = this.splitTableGuids(new ArrayList(this.tableGuids), splitNumber);
            for (List<String> files : splitedSourceFiles) {
                Configuration splitedConfig = this.originConfig.clone();
                splitedConfig.set(Key.TABLEGUIDS, files);
                readerSplitConfigs.add(splitedConfig);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        private <T> List<List<T>> splitTableGuids(final List<T> sourceList, int adviceNumber) {
            List<List<T>> splitedList = new ArrayList<List<T>>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }


    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private IAcsClient client=null;
        private List<String> tableGuids=new ArrayList<String>();;

        // dw链接参数
        private String regionid;
        private String accessKeyId;
        private String accessSecret;
        private String product;
        private String endpoint;
        private String appguids;


        @Override
        public void init() {
            this.readerSliceConfig = this.getPluginJobConf();
            this.regionid = this.readerSliceConfig.getString(Key.REGIONID);
            this.accessKeyId = this.readerSliceConfig.getString(Key.ACCESSKEYID);
            this.accessSecret = this.readerSliceConfig.getString(Key.ACCESSSECRET);
            this.product = this.readerSliceConfig.getString(Key.PRODUCT);
            this.endpoint = this.readerSliceConfig.getString(Key.ENDPOINT);
            this.appguids = this.readerSliceConfig.getString(Key.APPGUIDS);
            this.tableGuids=this.readerSliceConfig.getList(Key.TABLEGUIDS, String.class);
            IClientProfile profile = DefaultProfile.getProfile(this.regionid, this.accessKeyId, this.accessSecret);
            DefaultProfile.addEndpoint(this.regionid,this.product, this.endpoint);
            this.client = new DefaultAcsClient(profile);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.debug("start read tables...");
            for (String tableguid : this.tableGuids) {
                LOG.info(String.format("reading tableguid : [%s]", tableguid));

                //获取TableBasicInfo
                Record record = recordSender.createRecord();
                record.addColumn(new StringColumn("TableBasicInfo"));
                record.addColumn(new StringColumn(DwmetaInfo.getTableBasicInfo(this.client,tableguid)));
                recordSender.sendToWriter(record);

                //获取TableFullInfo
                Record TableFullInfoRecord = recordSender.createRecord();
                TableFullInfoRecord.addColumn(new StringColumn("TableFullInfo"));
                TableFullInfoRecord.addColumn(new StringColumn(DwmetaInfo.getTableFullInfo(this.client,tableguid)));
                recordSender.sendToWriter(TableFullInfoRecord);

            }

            LOG.debug("end read tables...");
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}