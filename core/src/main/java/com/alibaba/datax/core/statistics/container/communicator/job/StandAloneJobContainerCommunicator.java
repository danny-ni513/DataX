package com.alibaba.datax.core.statistics.container.communicator.job;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.collector.ProcessInnerCollector;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.report.ProcessInnerReporter;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.fastjson.JSON;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class StandAloneJobContainerCommunicator extends AbstractContainerCommunicator {
    private static final Logger LOG = LoggerFactory
            .getLogger(StandAloneJobContainerCommunicator.class);
    private String killSelfCheckUrl;
    private OkHttpClient okHttpClient;
    private boolean reportFlag;
    private String jobName;

    public StandAloneJobContainerCommunicator(Configuration configuration) {
        super(configuration);
        super.setCollector(new ProcessInnerCollector(configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID)));
        super.setReporter(new ProcessInnerReporter());
        this.killSelfCheckUrl = configuration.getString(CoreConstant.DATAX_CORE_KILL_SELF_CHECK_URL,"");
        if(StringUtils.isNotBlank(this.killSelfCheckUrl)){
            this.okHttpClient = new OkHttpClient();
        }
        this.reportFlag = configuration.getBool(CoreConstant.DATAX_CORE_JOB_REPORT_FLAG,true);
        this.jobName = configuration.getString(CoreConstant.JOB_NAME,"");
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        super.getCollector().registerTGCommunication(configurationList);
    }

    @Override
    public Communication collect() {
        return super.getCollector().collectFromTaskGroup();
    }

    @Override
    public State collectState() {
        return this.collect().getState();
    }

    /**
     * 和 DistributeJobContainerCollector 的 report 实现一样
     */
    @Override
    public void report(Communication communication) {
        super.getReporter().reportJobCommunication(super.getJobId(), communication);

        LOG.info(CommunicationTool.Stringify.getSnapshot(communication));
        reportToKafka(communication);
        reportVmInfo();
        /** 定时检查信息 **/
        if(StringUtils.isNotBlank(this.killSelfCheckUrl)&&this.okHttpClient!=null){
            Request request = new Request.Builder()
                    .url(this.killSelfCheckUrl)
                    .build();
            try (Response response = this.okHttpClient.newCall(request).execute()) {
                String killJobIds = response.body().string();
                String[] killJobIdArray = killJobIds.split(",");
                if(Arrays.stream(killJobIdArray)
                        .filter(f-> this.getJobId().toString().equals(f)).count()>0){
                    LOG.info("started to kill self...");

                    if(this.reportFlag){
                        String noticeId = this.getConfiguration().getString(CoreConstant.JOB_NOTICE_ID,"");
                        Map reportMap = new HashMap();
                        reportMap.put("notice_id",noticeId);
                        reportMap.put("job_name",this.jobName);
                        reportMap.put("job_id",this.getJobId());
                        reportMap.put("type", CoreConstant.JOB_NOTICE_TYPE_KILLED);
                        reportMap.put("now_timestamp",System.currentTimeMillis());
                        String jsonInfo = JSON.toJSONString(reportMap);
                        LOG.error(jsonInfo);
                    }

                    try{
                        Thread.sleep(3000);
                    }catch (Exception ex){
                        for(int i=0;i<30000;i++){
                        }
                    }
                    LOG.info("kill self now...");

                    System.exit(0);

                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
                this.killSelfCheckUrl = "";
            }

        }
    }

    private void reportToKafka(Communication communication){
        if(this.reportFlag){
            Map<String,Object> map = new HashMap<>(13);
            map.put("job_name",this.jobName);
            map.put("job_id",this.getJobId());
            map.put("now_timestamp",System.currentTimeMillis());
            map.putAll(CommunicationTool.Stringify.getSnapshotMap(communication));
            String jsonInfo = JSON.toJSONString(map);
            LOG.error(jsonInfo);
        }
    }

    @Override
    public Communication getCommunication(Integer taskGroupId) {
        return super.getCollector().getTGCommunication(taskGroupId);
    }

    @Override
    public Map<Integer, Communication> getCommunicationMap() {
        return super.getCollector().getTGCommunicationMap();
    }
}
