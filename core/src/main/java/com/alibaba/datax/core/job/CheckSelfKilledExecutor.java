package com.alibaba.datax.core.job;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author bingobing
 */
public class CheckSelfKilledExecutor implements Runnable{
    private static final Logger LOG = LoggerFactory
            .getLogger(CheckSelfKilledExecutor.class);
    private String killSelfCheckUrl;
    private OkHttpClient okHttpClient;
    private String jobName;
    private boolean reportFlag;
    private long jobId;
    private String idmId;
    private Configuration configuration;

    public CheckSelfKilledExecutor(Configuration configuration){
        this.configuration = configuration;
        this.jobId = configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        this.killSelfCheckUrl = configuration.getString(CoreConstant.DATAX_CORE_KILL_SELF_CHECK_URL,"");
        if(StringUtils.isNotBlank(this.killSelfCheckUrl)){
            this.okHttpClient = new OkHttpClient();
        }
        this.reportFlag = configuration.getBool(CoreConstant.DATAX_CORE_JOB_REPORT_FLAG,true);
        this.jobName = configuration.getString(CoreConstant.JOB_NAME,"JOB-"+this.jobId);
        this.idmId = configuration.getString(CoreConstant.JOB_IDM_ID,"");
    }

    @Override
    public void run() {
        assert null != this.configuration;
        Thread.currentThread().setName(
                String.format("checkSelfKilled-%d", this.jobId));
        /** 定时检查自杀信息 **/
        if(StringUtils.isNotBlank(this.killSelfCheckUrl)&&this.okHttpClient!=null){

                LOG.debug("checkSelfLive-[{}]", LocalDateTime.now());
            Request request = new Request.Builder()
                    .url(this.killSelfCheckUrl)
                    .build();
            try (Response response = this.okHttpClient.newCall(request).execute()) {
                String killJobIds = response.body().string();
                String[] killJobIdArray = killJobIds.split(",");
                if (Arrays.stream(killJobIdArray)
                        .filter(f -> (this.jobId + "").equals(f)).count() > 0) {
                    LOG.info("started to kill self...");

                    if (this.reportFlag) {
                        String noticeId = this.configuration.getString(CoreConstant.JOB_NOTICE_ID, "");
                        Map reportMap = new HashMap(5);
                        reportMap.put("notice_id", noticeId);
                        reportMap.put("job_name", this.jobName);
                        reportMap.put("job_id", this.jobId);
                        reportMap.put("idm_id",this.idmId);
                        reportMap.put("type", CoreConstant.JOB_NOTICE_TYPE_KILLED);
                        reportMap.put("now_timestamp", System.currentTimeMillis());
                        String jsonInfo = JSON.toJSONString(reportMap);
                        LOG.error(jsonInfo);
                    }

                    try {
                        Thread.sleep(3000);
                    } catch (Exception ex) {
                        for (int i = 0; i < 60000; i++) {}
                    }
                    LOG.info("kill self now...");

                    System.exit(0);

                }

            } catch (IOException e) {
                LOG.error(e.getMessage());
                //this.killSelfCheckUrl = "";
            }
        }
    }
}
