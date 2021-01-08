package com.alibaba.datax.core.job;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.JobPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.core.container.util.HookInvoker;
import com.alibaba.datax.core.container.util.JobAssignUtil;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.job.scheduler.processinner.StandAloneScheduler;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.communicator.job.StandAloneJobContainerCommunicator;
import com.alibaba.datax.core.statistics.plugin.DefaultJobPluginCollector;
import com.alibaba.datax.core.util.ErrorRecordChecker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.ClassLoaderSwapper;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.ExecuteMode;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *
 * @author jingxing
 * @date 14-8-24
 * <p/>
 * job实例运行在jobContainer容器中，它是所有任务的master，负责初始化、拆分、调度、运行、回收、监控和汇报
 * 但它并不做实际的数据同步操作
 */
public class JobContainer extends AbstractContainer {
    private static final Logger LOG = LoggerFactory
            .getLogger(JobContainer.class);

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");

    private ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper
            .newCurrentThreadClassLoaderSwapper();

    private long jobId;

    private String jobName;

    private String idmId;

    private String coreConfigServerUrl;

    private boolean withNacos;

    private  OkHttpClient okHttpClient;

    private String readerPluginName;

    private String writerPluginName;

    /**
     * reader和writer jobContainer的实例
     */
    private Reader.Job jobReader;

    private Writer.Job jobWriter;

    private Configuration userConf;

    private long startTimeStamp;

    private long endTimeStamp;

    private long startTransferTimeStamp;

    private long endTransferTimeStamp;

    private int needChannelNumber;

    private int totalStage = 1;

    private ErrorRecordChecker errorLimit;

    private ScheduledExecutorService checkSelfKilledService;

    private Gson readGson ;//= new GsonBuilder().create()

    public JobContainer(Configuration configuration) {
        super(configuration);
        errorLimit = new ErrorRecordChecker(configuration);
    }

    /**
     * jobContainer主要负责的工作全部在start()里面，包括init、prepare、split、scheduler、
     * post以及destroy和statistics
     */
    @Override
    public void start() {
        LOG.info("DataX jobContainer starts job.");
        this.jobId = this.configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID,-1);
        this.jobName = this.configuration.getString(CoreConstant.JOB_NAME,"JOB-"+this.jobId);
        this.idmId = this.configuration.getString(CoreConstant.JOB_IDM_ID,"");
        this.withNacos = this.configuration.getBool(CoreConstant.JOB_WITH_NACOS,false);
        this.coreConfigServerUrl = configuration.getString(CoreConstant.DATAX_CORE_CONFIG_SERVER_URL,"");
        /** 如果配置中心URL是有效，则检查所有配置中是否存在"[xxx.xxx;IDM_PARA.XXX.GET/ADD/MINUS;IDM_ID.XXX.GET/CREATE]" **/
        if(this.withNacos && StringUtils.isNotBlank(this.coreConfigServerUrl)
                && (this.coreConfigServerUrl.startsWith("http://")||this.coreConfigServerUrl.startsWith("https://"))){
            readGson = new GsonBuilder().create();
            initConfigWithNacos();
        }

        this.jobId = this.configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID,-1);
        sendNoticeMsg(new HashMap(){{
            put("type", CoreConstant.JOB_NOTICE_TYPE_START);
        }});

        String killSelfCheckUrl = configuration.getString(CoreConstant.DATAX_CORE_KILL_SELF_CHECK_URL,"");

        if(org.apache.commons.lang3.StringUtils.isNotBlank(killSelfCheckUrl)){
            this.checkSelfKilledService = Executors.newSingleThreadScheduledExecutor();
            this.checkSelfKilledService.scheduleWithFixedDelay(
                    new CheckSelfKilledExecutor(this.configuration),1,1, TimeUnit.SECONDS
            );
        }

        boolean hasException = false;
        boolean isDryRun = false;
        try {
            this.startTimeStamp = System.currentTimeMillis();
            isDryRun = configuration.getBool(CoreConstant.DATAX_JOB_SETTING_DRYRUN, false);
            if(isDryRun) {
                LOG.info("jobContainer starts to do preCheck ...");
                this.preCheck();
            } else {
                userConf = configuration.clone();
                LOG.debug("jobContainer starts to do preHandle ...");
                this.preHandle();

                LOG.debug("jobContainer starts to do init ...");
                this.init();
                LOG.info("jobContainer starts to do prepare ...");
                this.prepare();
                LOG.info("jobContainer starts to do split ...");
                this.totalStage = this.split();
                LOG.info("jobContainer starts to do schedule ...");
                this.schedule();
                LOG.debug("jobContainer starts to do post ...");
                this.post();

                LOG.debug("jobContainer starts to do postHandle ...");
                this.postHandle();
                LOG.info("DataX jobId [{}] completed successfully.", this.jobId);

                this.invokeHooks();
            }
        } catch (Throwable e) {
            LOG.error("Exception when job run", e);

            sendNoticeMsg(new HashMap<String,String>(){{
                put("type", CoreConstant.JOB_NOTICE_TYPE_INIT_ERROR);
                put("msg","Exception when job run");
            }});
            hasException = true;

            if (e instanceof OutOfMemoryError) {
                this.destroy();
                System.gc();
            }


            if (super.getContainerCommunicator() == null) {
                // 由于 containerCollector 是在 scheduler() 中初始化的，所以当在 scheduler() 之前出现异常时，需要在此处对 containerCollector 进行初始化

                AbstractContainerCommunicator tempContainerCollector;
                // standalone
                tempContainerCollector = new StandAloneJobContainerCommunicator(configuration);

                super.setContainerCommunicator(tempContainerCollector);
            }

            Communication communication = super.getContainerCommunicator().collect();
            // 汇报前的状态，不需要手动进行设置
            // communication.setState(State.FAILED);
            communication.setThrowable(e);
            communication.setTimestamp(this.endTimeStamp);

            Communication tempComm = new Communication();
            tempComm.setTimestamp(this.startTransferTimeStamp);

            Communication reportCommunication = CommunicationTool.getReportCommunication(communication, tempComm, this.totalStage);
            super.getContainerCommunicator().report(reportCommunication);

            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        } finally {
            if(!isDryRun) {

                this.destroy();
                this.endTimeStamp = System.currentTimeMillis();
                if (!hasException) {
                    //先打印任务统计结果
                    this.logStatistics();

                    //最后打印cpu的平均消耗，GC的统计
                    VMInfo vmInfo = VMInfo.getVmInfo();
                    if (vmInfo != null) {
                        vmInfo.getDelta(false);
                        LOG.info(vmInfo.totalString());
                    }

                    LOG.info(PerfTrace.getInstance().summarizeNoException());
                    int waitDestroy = 0;
                    for(int i=0;i<10*10000;i++){
                        if(i%10000 == 0){
                            waitDestroy++;
                            LOG.info("回收等待--->{}",waitDestroy);
                        }
                    }

                    try{
                        Thread.sleep(1*1000);

                    }catch (Exception exception){
                        LOG.error("sleep error");
                    }

                }
            }
        }
    }

    private void preCheck() {
        this.preCheckInit();
        this.adjustChannelNumber();

        if (this.needChannelNumber <= 0) {
            this.needChannelNumber = 1;
        }
        this.preCheckReader();
        this.preCheckWriter();
        LOG.info("PreCheck通过");
    }

    private void preCheckInit() {
        this.jobId = this.configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, -1);

        if (this.jobId < 0) {
            LOG.info("Set jobId = 0");
            this.jobId = 0;
            this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID,
                    this.jobId);
        }

        Thread.currentThread().setName("job-" + this.jobId);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                this.getContainerCommunicator());
        this.jobReader = this.preCheckReaderInit(jobPluginCollector);
        this.jobWriter = this.preCheckWriterInit(jobPluginCollector);
    }

    private Reader.Job preCheckReaderInit(JobPluginCollector jobPluginCollector) {
        this.readerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));

        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(
                PluginType.READER, this.readerPluginName);

        this.configuration.set(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER + ".dryRun", true);

        // 设置reader的jobConfig
        jobReader.setPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));
        // 设置reader的readerConfig
        jobReader.setPeerPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        jobReader.setJobPluginCollector(jobPluginCollector);

        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }


    private Writer.Job preCheckWriterInit(JobPluginCollector jobPluginCollector) {
        this.writerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));

        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(
                PluginType.WRITER, this.writerPluginName);

        this.configuration.set(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER + ".dryRun", true);

        // 设置writer的jobConfig
        jobWriter.setPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));
        // 设置reader的readerConfig
        jobWriter.setPeerPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        jobWriter.setPeerPluginName(this.readerPluginName);
        jobWriter.setJobPluginCollector(jobPluginCollector);

        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobWriter;
    }

    private void preCheckReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        LOG.info(String.format("DataX Reader.Job [%s] do preCheck work .",
                this.readerPluginName));
        this.jobReader.preCheck();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void preCheckWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));
        LOG.info(String.format("DataX Writer.Job [%s] do preCheck work .",
                this.writerPluginName));
        this.jobWriter.preCheck();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /**
     * reader和writer的初始化
     */
    private void init() {
        this.jobId = this.configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, -1);

        if (this.jobId < 0) {
            LOG.info("Set jobId = 0");
            this.jobId = 0;
            this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID,
                    this.jobId);
        }

        Thread.currentThread().setName("job-" + this.jobId);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                this.getContainerCommunicator());
        //必须先Reader ，后Writer
        this.jobReader = this.initJobReader(jobPluginCollector);
        this.jobWriter = this.initJobWriter(jobPluginCollector);
    }

    private void prepare() {
        this.prepareJobReader();
        this.prepareJobWriter();
    }

    private void preHandle() {
        String handlerPluginTypeStr = this.configuration.getString(
                CoreConstant.DATAX_JOB_PREHANDLER_PLUGINTYPE);
        if(!StringUtils.isNotEmpty(handlerPluginTypeStr)){
            return;
        }
        PluginType handlerPluginType;
        try {
            handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.CONFIG_ERROR,
                    String.format("Job preHandler's pluginType(%s) set error, reason(%s)", handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        String handlerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                handlerPluginType, handlerPluginName));

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(
                handlerPluginType, handlerPluginName);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                this.getContainerCommunicator());
        handler.setJobPluginCollector(jobPluginCollector);

        //todo configuration的安全性，将来必须保证
        handler.preHandler(configuration);
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        LOG.info("After PreHandler: \n" + Engine.filterJobConfiguration(configuration) + "\n");
    }

    private void postHandle() {
        String handlerPluginTypeStr = this.configuration.getString(
                CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINTYPE);

        if(!StringUtils.isNotEmpty(handlerPluginTypeStr)){
            return;
        }
        PluginType handlerPluginType;
        try {
            handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.CONFIG_ERROR,
                    String.format("Job postHandler's pluginType(%s) set error, reason(%s)", handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        String handlerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINNAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                handlerPluginType, handlerPluginName));

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(
                handlerPluginType, handlerPluginName);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                this.getContainerCommunicator());
        handler.setJobPluginCollector(jobPluginCollector);

        handler.postHandler(configuration);
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }


    /**
     * 执行reader和writer最细粒度的切分，需要注意的是，writer的切分结果要参照reader的切分结果，
     * 达到切分后数目相等，才能满足1：1的通道模型，所以这里可以将reader和writer的配置整合到一起，
     * 然后，为避免顺序给读写端带来长尾影响，将整合的结果shuffler掉
     */
    private int split() {
        this.adjustChannelNumber();

        if (this.needChannelNumber <= 0) {
            this.needChannelNumber = 1;
        }

        List<Configuration> readerTaskConfigs = this
                .doReaderSplit(this.needChannelNumber);
        int taskNumber = readerTaskConfigs.size();
        List<Configuration> writerTaskConfigs = this
                .doWriterSplit(taskNumber);

        List<Configuration> transformerList = this.configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT_TRANSFORMER);

        LOG.debug("transformer configuration: "+ JSON.toJSONString(transformerList));
        /**
         * 输入是reader和writer的parameter list，输出是content下面元素的list
         */
        List<Configuration> contentConfig = mergeReaderAndWriterTaskConfigs(
                readerTaskConfigs, writerTaskConfigs, transformerList);


        LOG.debug("contentConfig configuration: "+ JSON.toJSONString(contentConfig));

        this.configuration.set(CoreConstant.DATAX_JOB_CONTENT, contentConfig);

        return contentConfig.size();
    }

    private void adjustChannelNumber() {
        int needChannelNumberByByte = Integer.MAX_VALUE;
        int needChannelNumberByRecord = Integer.MAX_VALUE;

        boolean isByteLimit = (this.configuration.getInt(
                CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 0) > 0);
        if (isByteLimit) {
            long globalLimitedByteSpeed = this.configuration.getInt(
                    CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 10 * 1024 * 1024);

            // 在byte流控情况下，单个Channel流量最大值必须设置，否则报错！
            Long channelLimitedByteSpeed = this.configuration
                    .getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE);
            if (channelLimitedByteSpeed == null || channelLimitedByteSpeed <= 0) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.CONFIG_ERROR,
                        "在有总bps限速条件下，单个channel的bps值不能为空，也不能为非正数");
            }

            needChannelNumberByByte =
                    (int) (globalLimitedByteSpeed / channelLimitedByteSpeed);
            needChannelNumberByByte =
                    needChannelNumberByByte > 0 ? needChannelNumberByByte : 1;
            LOG.info("Job set Max-Byte-Speed to " + globalLimitedByteSpeed + " bytes.");
        }

        boolean isRecordLimit = (this.configuration.getInt(
                CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 0)) > 0;
        if (isRecordLimit) {
            long globalLimitedRecordSpeed = this.configuration.getInt(
                    CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 100000);

            Long channelLimitedRecordSpeed = this.configuration.getLong(
                    CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_RECORD);
            if (channelLimitedRecordSpeed == null || channelLimitedRecordSpeed <= 0) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                        "在有总tps限速条件下，单个channel的tps值不能为空，也不能为非正数");
            }

            needChannelNumberByRecord =
                    (int) (globalLimitedRecordSpeed / channelLimitedRecordSpeed);
            needChannelNumberByRecord =
                    needChannelNumberByRecord > 0 ? needChannelNumberByRecord : 1;
            LOG.info("Job set Max-Record-Speed to " + globalLimitedRecordSpeed + " records.");
        }

        // 取较小值
        this.needChannelNumber = needChannelNumberByByte < needChannelNumberByRecord ?
                needChannelNumberByByte : needChannelNumberByRecord;

        // 如果从byte或record上设置了needChannelNumber则退出
        if (this.needChannelNumber < Integer.MAX_VALUE) {
            return;
        }

        boolean isChannelLimit = (this.configuration.getInt(
                CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL, 0) > 0);
        if (isChannelLimit) {
            this.needChannelNumber = this.configuration.getInt(
                    CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL);

            LOG.info("Job set Channel-Number to " + this.needChannelNumber
                    + " channels.");

            return;
        }

        throw DataXException.asDataXException(
                FrameworkErrorCode.CONFIG_ERROR,
                "Job运行速度必须设置");
    }

    /**
     * schedule首先完成的工作是把上一步reader和writer split的结果整合到具体taskGroupContainer中,
     * 同时不同的执行模式调用不同的调度策略，将所有任务调度起来
     */
    private void schedule() {
        /**
         * 这里的全局speed和每个channel的速度设置为B/s
         */
        int channelsPerTaskGroup = this.configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, 5);
        int taskNumber = this.configuration.getList(
                CoreConstant.DATAX_JOB_CONTENT).size();

        this.needChannelNumber = Math.min(this.needChannelNumber, taskNumber);
        PerfTrace.getInstance().setChannelNumber(needChannelNumber);

        /**
         * 通过获取配置信息得到每个taskGroup需要运行哪些tasks任务
         */

        List<Configuration> taskGroupConfigs = JobAssignUtil.assignFairly(this.configuration,
                this.needChannelNumber, channelsPerTaskGroup);

        LOG.info("Scheduler starts [{}] taskGroups.", taskGroupConfigs.size());

        ExecuteMode executeMode = null;
        AbstractScheduler scheduler;
        try {
        	executeMode = ExecuteMode.STANDALONE;
            scheduler = initStandaloneScheduler(this.configuration);

            //设置 executeMode
            for (Configuration taskGroupConfig : taskGroupConfigs) {
                taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, executeMode.getValue());
            }

            if (executeMode == ExecuteMode.LOCAL || executeMode == ExecuteMode.DISTRIBUTE) {
                if (this.jobId <= 0) {
                    throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
                            "在[ local | distribute ]模式下必须设置jobId，并且其值 > 0 .");
                }
            }

            LOG.info("Running by {} Mode.", executeMode);

            this.startTransferTimeStamp = System.currentTimeMillis();

            scheduler.schedule(taskGroupConfigs);

            this.endTransferTimeStamp = System.currentTimeMillis();
        } catch (Exception e) {
            LOG.error("运行scheduler 模式[{}]出错.", executeMode);
            this.endTransferTimeStamp = System.currentTimeMillis();
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }

        /**
         * 检查任务执行情况
         */
        this.checkLimit();
    }


    private AbstractScheduler initStandaloneScheduler(Configuration configuration) {
        AbstractContainerCommunicator containerCommunicator = new StandAloneJobContainerCommunicator(configuration);
        super.setContainerCommunicator(containerCommunicator);

        return new StandAloneScheduler(containerCommunicator);
    }

    private void post() {
        this.postJobWriter();
        this.postJobReader();
    }

    private void destroy() {
        if (this.jobWriter != null) {
            this.jobWriter.destroy();
            this.jobWriter = null;
        }
        if (this.jobReader != null) {
            this.jobReader.destroy();
            this.jobReader = null;
        }
    }

    private void logStatistics() {
        long totalCosts = (this.endTimeStamp - this.startTimeStamp) / 1000;
        long transferCosts = (this.endTransferTimeStamp - this.startTransferTimeStamp) / 1000;
        if (0L == transferCosts) {
            transferCosts = 1L;
        }

        if (super.getContainerCommunicator() == null) {
            return;
        }

        Communication communication = super.getContainerCommunicator().collect();
        communication.setTimestamp(this.endTimeStamp);

        Communication tempComm = new Communication();
        tempComm.setTimestamp(this.startTransferTimeStamp);

        Communication reportCommunication = CommunicationTool.getReportCommunication(communication, tempComm, this.totalStage);

        // 字节速率
        long byteSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES)
                / transferCosts;

        long recordSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS)
                / transferCosts;

        reportCommunication.setLongCounter(CommunicationTool.BYTE_SPEED, byteSpeedPerSecond);
        reportCommunication.setLongCounter(CommunicationTool.RECORD_SPEED, recordSpeedPerSecond);

        super.getContainerCommunicator().report(reportCommunication);


        LOG.info(String.format(
                "\n" + "%-26s: %-18s\n" + "%-26s: %-18s\n" + "%-26s: %19s\n"
                        + "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n"
                        + "%-26s: %19s\n",
                "任务启动时刻",
                dateFormat.format(startTimeStamp),

                "任务结束时刻",
                dateFormat.format(endTimeStamp),

                "任务总计耗时",
                String.valueOf(totalCosts) + "s",
                "任务平均流量",
                StrUtil.stringify(byteSpeedPerSecond)
                        + "/s",
                "记录写入速度",
                String.valueOf(recordSpeedPerSecond)
                        + "rec/s", "读出记录总数",
                String.valueOf(CommunicationTool.getTotalReadRecords(communication)),
                "读写失败总数",
                String.valueOf(CommunicationTool.getTotalErrorRecords(communication))
        ));

        sendNoticeMsg(new HashMap<String,String>(){{
            put("type", CoreConstant.JOB_NOTICE_TYPE_FINISH);
            put("start_timestamp",dateFormat.format(startTimeStamp));
            put("end_timestamp",dateFormat.format(endTimeStamp));
            put("total_costs",totalCosts+"");
            put("byte_speed_per_second",byteSpeedPerSecond+"");
            put("record_speed_pre_second",recordSpeedPerSecond+"");
            put("total_read_record_num",CommunicationTool.getTotalReadRecords(communication)+"");
            put("total_write_fail_record_num",CommunicationTool.getTotalErrorRecords(communication)+"");
        }});

        if (communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS) > 0
                || communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS) > 0
                || communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS) > 0) {
            LOG.info(String.format(
                    "\n" + "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n",
                    "Transformer成功记录总数",
                    communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS),

                    "Transformer失败记录总数",
                    communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS),

                    "Transformer过滤记录总数",
                    communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS)
            ));
        }


    }

    /**
     * reader job的初始化，返回Reader.Job
     *
     * @return
     */
    private Reader.Job initJobReader(
            JobPluginCollector jobPluginCollector) {
        this.readerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));

        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(
                PluginType.READER, this.readerPluginName);

        // 设置reader的jobConfig
        jobReader.setPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        // 设置reader的readerConfig
        jobReader.setPeerPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));

        jobReader.setJobPluginCollector(jobPluginCollector);
        jobReader.init();

        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }

    /**
     * writer job的初始化，返回Writer.Job
     *
     * @return
     */
    private Writer.Job initJobWriter(
            JobPluginCollector jobPluginCollector) {
        this.writerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));

        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(
                PluginType.WRITER, this.writerPluginName);

        // 设置writer的jobConfig
        jobWriter.setPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));

        // 设置reader的readerConfig
        jobWriter.setPeerPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        jobWriter.setPeerPluginName(this.readerPluginName);
        jobWriter.setJobPluginCollector(jobPluginCollector);
        jobWriter.init();
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobWriter;
    }

    private void prepareJobReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        LOG.info(String.format("DataX Reader.Job [%s] do prepare work .",
                this.readerPluginName));
        this.jobReader.prepare();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void prepareJobWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));
        LOG.info(String.format("DataX Writer.Job [%s] do prepare work .",
                this.writerPluginName));
        this.jobWriter.prepare();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    // TODO: 如果源头就是空数据
    private List<Configuration> doReaderSplit(int adviceNumber) {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        List<Configuration> readerSlicesConfigs =
                this.jobReader.split(adviceNumber);
        if (readerSlicesConfigs == null || readerSlicesConfigs.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    "reader切分的task数目不能小于等于0");
        }
        LOG.info("DataX Reader.Job [{}] splits to [{}] tasks.",
                this.readerPluginName, readerSlicesConfigs.size());
        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return readerSlicesConfigs;
    }

    private List<Configuration> doWriterSplit(int readerTaskNumber) {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));

        List<Configuration> writerSlicesConfigs = this.jobWriter
                .split(readerTaskNumber);
        if (writerSlicesConfigs == null || writerSlicesConfigs.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    "writer切分的task不能小于等于0");
        }
        LOG.info("DataX Writer.Job [{}] splits to [{}] tasks.",
                this.writerPluginName, writerSlicesConfigs.size());
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return writerSlicesConfigs;
    }

    /**
     * 按顺序整合reader和writer的配置，这里的顺序不能乱！ 输入是reader、writer级别的配置，输出是一个完整task的配置
     *
     * @param readerTasksConfigs
     * @param writerTasksConfigs
     * @return
     */
    private List<Configuration> mergeReaderAndWriterTaskConfigs(
            List<Configuration> readerTasksConfigs,
            List<Configuration> writerTasksConfigs) {
        return mergeReaderAndWriterTaskConfigs(readerTasksConfigs, writerTasksConfigs, null);
    }

    private List<Configuration> mergeReaderAndWriterTaskConfigs(
            List<Configuration> readerTasksConfigs,
            List<Configuration> writerTasksConfigs,
            List<Configuration> transformerConfigs) {
        if (readerTasksConfigs.size() != writerTasksConfigs.size()) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    String.format("reader切分的task数目[%d]不等于writer切分的task数目[%d].",
                            readerTasksConfigs.size(), writerTasksConfigs.size())
            );
        }

        List<Configuration> contentConfigs = new ArrayList<Configuration>();
        for (int i = 0; i < readerTasksConfigs.size(); i++) {
            Configuration taskConfig = Configuration.newDefault();
            taskConfig.set(CoreConstant.JOB_READER_NAME,
                    this.readerPluginName);
            taskConfig.set(CoreConstant.JOB_READER_PARAMETER,
                    readerTasksConfigs.get(i));
            taskConfig.set(CoreConstant.JOB_WRITER_NAME,
                    this.writerPluginName);
            taskConfig.set(CoreConstant.JOB_WRITER_PARAMETER,
                    writerTasksConfigs.get(i));

            if(transformerConfigs!=null && transformerConfigs.size()>0){
                taskConfig.set(CoreConstant.JOB_TRANSFORMER, transformerConfigs);
            }

            taskConfig.set(CoreConstant.TASK_ID, i);
            contentConfigs.add(taskConfig);
        }

        return contentConfigs;
    }

    /**
     * 这里比较复杂，分两步整合 1. tasks到channel 2. channel到taskGroup
     * 合起来考虑，其实就是把tasks整合到taskGroup中，需要满足计算出的channel数，同时不能多起channel
     * <p/>
     * example:
     * <p/>
     * 前提条件： 切分后是1024个分表，假设用户要求总速率是1000M/s，每个channel的速率的3M/s，
     * 每个taskGroup负责运行7个channel
     * <p/>
     * 计算： 总channel数为：1000M/s / 3M/s =
     * 333个，为平均分配，计算可知有308个每个channel有3个tasks，而有25个每个channel有4个tasks，
     * 需要的taskGroup数为：333 / 7 =
     * 47...4，也就是需要48个taskGroup，47个是每个负责7个channel，有4个负责1个channel
     * <p/>
     * 处理：我们先将这负责4个channel的taskGroup处理掉，逻辑是：
     * 先按平均为3个tasks找4个channel，设置taskGroupId为0，
     * 接下来就像发牌一样轮询分配task到剩下的包含平均channel数的taskGroup中
     * <p/>
     * TODO delete it
     *
     * @param averTaskPerChannel
     * @param channelNumber
     * @param channelsPerTaskGroup
     * @return 每个taskGroup独立的全部配置
     */
    @SuppressWarnings("serial")
    private List<Configuration> distributeTasksToTaskGroup(
            int averTaskPerChannel, int channelNumber,
            int channelsPerTaskGroup) {
        Validate.isTrue(averTaskPerChannel > 0 && channelNumber > 0
                        && channelsPerTaskGroup > 0,
                "每个channel的平均task数[averTaskPerChannel]，channel数目[channelNumber]，每个taskGroup的平均channel数[channelsPerTaskGroup]都应该为正数");
        List<Configuration> taskConfigs = this.configuration
                .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
        int taskGroupNumber = channelNumber / channelsPerTaskGroup;
        int leftChannelNumber = channelNumber % channelsPerTaskGroup;
        if (leftChannelNumber > 0) {
            taskGroupNumber += 1;
        }

        /**
         * 如果只有一个taskGroup，直接打标返回
         */
        if (taskGroupNumber == 1) {
            final Configuration taskGroupConfig = this.configuration.clone();
            /**
             * configure的clone不能clone出
             */
            taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, this.configuration
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT));
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                    channelNumber);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 0);
            return new ArrayList<Configuration>() {
                {
                    add(taskGroupConfig);
                }
            };
        }

        List<Configuration> taskGroupConfigs = new ArrayList<Configuration>();
        /**
         * 将每个taskGroup中content的配置清空
         */
        for (int i = 0; i < taskGroupNumber; i++) {
            Configuration taskGroupConfig = this.configuration.clone();
            List<Configuration> taskGroupJobContent = taskGroupConfig
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
            taskGroupJobContent.clear();
            taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, taskGroupJobContent);

            taskGroupConfigs.add(taskGroupConfig);
        }

        int taskConfigIndex = 0;
        int channelIndex = 0;
        int taskGroupConfigIndex = 0;

        /**
         * 先处理掉taskGroup包含channel数不是平均值的taskGroup
         */
        if (leftChannelNumber > 0) {
            Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
            for (; channelIndex < leftChannelNumber; channelIndex++) {
                for (int i = 0; i < averTaskPerChannel; i++) {
                    List<Configuration> taskGroupJobContent = taskGroupConfig
                            .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
                    taskGroupJobContent.add(taskConfigs.get(taskConfigIndex++));
                    taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT,
                            taskGroupJobContent);
                }
            }

            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                    leftChannelNumber);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID,
                    taskGroupConfigIndex++);
        }

        /**
         * 下面需要轮询分配，并打上channel数和taskGroupId标记
         */
        int equalDivisionStartIndex = taskGroupConfigIndex;
        for (; taskConfigIndex < taskConfigs.size()
                && equalDivisionStartIndex < taskGroupConfigs.size(); ) {
            for (taskGroupConfigIndex = equalDivisionStartIndex; taskGroupConfigIndex < taskGroupConfigs
                    .size() && taskConfigIndex < taskConfigs.size(); taskGroupConfigIndex++) {
                Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
                List<Configuration> taskGroupJobContent = taskGroupConfig
                        .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
                taskGroupJobContent.add(taskConfigs.get(taskConfigIndex++));
                taskGroupConfig.set(
                        CoreConstant.DATAX_JOB_CONTENT, taskGroupJobContent);
            }
        }

        for (taskGroupConfigIndex = equalDivisionStartIndex;
             taskGroupConfigIndex < taskGroupConfigs.size(); ) {
            Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                    channelsPerTaskGroup);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID,
                    taskGroupConfigIndex++);
        }

        return taskGroupConfigs;
    }

    private void postJobReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        LOG.info("DataX Reader.Job [{}] do post work.",
                this.readerPluginName);
        this.jobReader.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void postJobWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));
        LOG.info("DataX Writer.Job [{}] do post work.",
                this.writerPluginName);
        this.jobWriter.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /**
     * 检查最终结果是否超出阈值，如果阈值设定小于1，则表示百分数阈值，大于1表示条数阈值。
     *
     * @param
     */
    private void checkLimit() {
        Communication communication = super.getContainerCommunicator().collect();
        errorLimit.checkRecordLimit(communication);
        errorLimit.checkPercentageLimit(communication);
    }

    /**
     * 调用外部hook
     */
    private void invokeHooks() {
        Communication comm = super.getContainerCommunicator().collect();
        HookInvoker invoker = new HookInvoker(CoreConstant.DATAX_HOME + "/hook", configuration, comm.getCounter());
        invoker.invokeAll();
    }

    private void sendNoticeMsg(Map map){
        boolean reportFlag=this.configuration.getBool(CoreConstant.DATAX_CORE_JOB_REPORT_FLAG,true);
        if(reportFlag){

            if(StringUtils.isBlank(jobName)){
                jobName = "JOB-"+this.jobId;
            }
            String noticeId=this.configuration.getString(CoreConstant.JOB_NOTICE_ID,"");
            map.put("notice_id",noticeId);
            map.put("job_name",jobName);
            map.put("idm_id",this.idmId);
            map.put("job_id",this.jobId);
            map.put("now_timestamp",System.currentTimeMillis());
            String jsonInfo = JSON.toJSONString(map);
            LOG.error(jsonInfo);
        }
    }

    /**
     * 通过配置中心NACOS初始化整个任务配置文件
     */
    private void initConfigWithNacos() throws DataXException {
        okHttpClient = new OkHttpClient();
        String coreConfigUrl = MessageFormat.format(this.coreConfigServerUrl, "core");
        Request request = new Request.Builder()
                .url(coreConfigUrl)
                .build();

        try (Response response = okHttpClient.newCall(request).execute()) {
            String coreConfigStr = response.body().string();
            Configuration nacosCoreConfig = Configuration.from(coreConfigStr);
            String idmParaUrl = nacosCoreConfig.getString(CoreConstant.NACOS_CORE_IDM_PARA_URL);
            String idmRandomIdUrl = nacosCoreConfig.getString(CoreConstant.NACOS_CORE_IDM_RANDOM_ID_URL);
            if(StringUtils.isNotBlank(this.idmId) && this.idmId.startsWith("[IDM_ID.")&&this.idmId.endsWith("]")){
                initIdmRandomId(idmRandomIdUrl);
            }
            //目前仅支持 content[0].reader.parameter.filter.value 和jobName ,
            String readerFilterValue = this.configuration.getString(CoreConstant.JOB_CONTENT_0_READER_PARAMETER_FILTER_VALUE,"");
            if(StringUtils.isNotBlank(readerFilterValue) && readerFilterValue.startsWith("[IDM_PARA.")
                    && readerFilterValue.endsWith("]")){
                Object paraValue = this.getIdmParaValue(idmParaUrl,readerFilterValue);
                this.configuration.set(CoreConstant.JOB_CONTENT_0_READER_PARAMETER_FILTER_VALUE,paraValue);
            }

            if(StringUtils.isNotBlank(this.jobName) && this.jobName.contains("[IDM_PARA.")
                    && this.jobName.contains("]")){
                String paraJobName = this.jobName.substring(this.jobName.indexOf("[IDM_PARA."),this.jobName.lastIndexOf("]")+1);
                Object paraValue = this.getIdmParaValue(idmParaUrl,paraJobName);
                this.jobName = this.jobName.replace(paraJobName,paraValue.toString());

                this.configuration.set(CoreConstant.JOB_NAME,this.jobName);
            }

            LOG.warn("======1======");
            //content[0].reader.parameter.where
            if(StringUtils.isNotBlank(this.configuration.getString(CoreConstant.JOB_CONTENT_0_READER_PARAMETER_WHERE
                    ,""))
                    && this.configuration.getString(CoreConstant.JOB_CONTENT_0_READER_PARAMETER_WHERE
                    ,"").contains("[IDM_PARA.")
                    && this.configuration.getString(CoreConstant.JOB_CONTENT_0_READER_PARAMETER_WHERE
                    ,"").contains("]")){
                String whereStr = this.configuration.getString(CoreConstant.JOB_CONTENT_0_READER_PARAMETER_WHERE
                        ,"");
                String wherePara = whereStr.substring(whereStr.indexOf("[IDM_PARA."),whereStr.lastIndexOf("]")+1);
                Object paraValue = this.getIdmParaValue(idmParaUrl,wherePara);
                whereStr = whereStr.replace(wherePara,paraValue.toString());
                this.configuration.set(CoreConstant.JOB_CONTENT_0_READER_PARAMETER_WHERE,whereStr);
            }
            LOG.warn("======2======");

            if(this.configuration.get(CoreConstant.JOB_CONTENT_0_READER_PARAMETER_CONNECTION)!=null){
                List<Configuration> connectionList = this.configuration
                        .getListConfiguration(CoreConstant.JOB_CONTENT_0_READER_PARAMETER_CONNECTION);

                LOG.warn("======2.0======connection,size=[{}]",connectionList.size());
                List<Configuration> newConnectionList = connectionList.stream().map(connection->{
                    LOG.warn("======2.1======");
                    LOG.warn(connection.beautify());
                    Configuration newConnection = connection;
                  if(connection.get(CoreConstant.QUERY_SQL)!=null){
                      List<String> querySqls = connection.getList(CoreConstant.QUERY_SQL,String.class);
                      LOG.warn("======2.2======");
                      LOG.warn(querySqls.toString());
                      List<String> newQuerySqls = querySqls.stream().map(sql->{
                          if(sql.contains("[IDM_PARA.")&&sql.contains("]")){
                              String paraTag = sql.substring(sql.indexOf("[IDM_PARA."),sql.lastIndexOf("]")+1);
                              Object paraValue = this.getIdmParaValue(idmParaUrl,paraTag);
                              String newSql = sql.replace(paraTag,paraValue.toString());
                              return newSql;
                          }else{
                             return sql;
                          }
                      }).collect(Collectors.toList());
                      newConnection.set(CoreConstant.QUERY_SQL,newQuerySqls);
                      return newConnection;
                  }else{
                      return connection;
                  }
                }).collect(Collectors.toList());
                this.configuration.set(CoreConstant.JOB_CONTENT_0_READER_PARAMETER_CONNECTION,newConnectionList);
            }

            LOG.warn("======3======");

            //READER 进行处理
            Configuration readerConfig = this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_0_READER_PARAMETER);
            Configuration finalReaderConfig = fillPluginConfigWithNacos(readerConfig);
            this.configuration.set(CoreConstant.JOB_CONTENT_0_READER_PARAMETER,finalReaderConfig);
            //WRITER 进行处理
            Configuration writerConfig = this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_0_WRITER_PARAMETER);
            Configuration finalWriterConfig = fillPluginConfigWithNacos(writerConfig);
            this.configuration.set(CoreConstant.JOB_CONTENT_0_WRITER_PARAMETER,finalWriterConfig);

        }catch (Exception ex){
            ex.printStackTrace();
            sendNoticeMsg(new HashMap(){{
                put("type", CoreConstant.JOB_NOTICE_TYPE_INIT_ERROR);
                put("msg","请求 nacos config 异常，任务初始化失败");
            }});
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,"请求 nacos config 异常，任务初始化失败");
        }
        LOG.warn("FINAL CONFIG---------");
        LOG.warn(this.configuration.beautify());
    }

    private void initIdmRandomId(String idmRandomIdUrl){
        String idmIdTag = this.idmId.substring(this.idmId.indexOf(".")+1,this.idmId.lastIndexOf("."));
        String url="";
        if(this.idmId.endsWith(".CREATE]")){
            url = MessageFormat.format(idmRandomIdUrl,"create",idmIdTag,new Random().nextInt(99));
        }else{
            url = MessageFormat.format(idmRandomIdUrl,"get",idmIdTag,new Random().nextInt(99));
        }
        Request idmRandomIdRequest = new Request.Builder()
                .url(url)
                .build();
        try (Response idmRandomIdResponse = okHttpClient.newCall(idmRandomIdRequest).execute()) {
            String idmRandomIdResponseBody = idmRandomIdResponse.body().string();
            LOG.info("idmRandomIdResponseBody=>[{}]",idmRandomIdResponseBody);
            Map<String,Object> map = readGson.fromJson(idmRandomIdResponseBody,Map.class);
            if(CoreConstant.IDM_CONFIG_RESPONSE_SUCCESS_CODE.equals(map.getOrDefault(CoreConstant.IDM_CONFIG_RESPONSE_CODE,"").toString())){
                this.idmId = map.getOrDefault(CoreConstant.IDM_CONFIG_RESPONSE_DATA,"").toString();
                this.configuration.set(CoreConstant.JOB_IDM_ID,this.idmId);
            }else{
                LOG.warn("idmRandomIdResponse error ",idmRandomIdResponse.body().string());
            }
        }catch (Exception idmRandomIdEx){
            LOG.warn("idmRandomIdResponse error ",idmRandomIdEx.getMessage());
        }
    }

    private Object getIdmParaValue(String idmParaUrl,String paraInfo) throws DataXException {
        String paraTag = paraInfo.substring(paraInfo.indexOf(".")+1,paraInfo.lastIndexOf("."));
        String optTag = paraInfo.substring(paraInfo.lastIndexOf(".")+1,paraInfo.lastIndexOf("]"));
        String url = MessageFormat.format(idmParaUrl,optTag,paraTag,new Random().nextInt(99));
        LOG.info("idmParaUrl =>[{}]",url);
        Request request = new Request.Builder()
                .url(url)
                .build();
        try (Response response = okHttpClient.newCall(request).execute()) {
            String paraConfigStr = response.body().string();
            LOG.info("paraConfig =>[{}]",paraConfigStr);
            Map<String,Object> paraConfig = readGson.fromJson(paraConfigStr,Map.class);
            if(CoreConstant.IDM_CONFIG_RESPONSE_SUCCESS_CODE.equals(paraConfig.getOrDefault(CoreConstant.IDM_CONFIG_RESPONSE_CODE,"").toString())){
                String paraValueStr = paraConfig.getOrDefault(CoreConstant.IDM_CONFIG_RESPONSE_DATA,"").toString();
                Object paraValue = null;
                if(StringUtils.isNotBlank(paraValueStr)&&StringUtils.isNumeric(paraValueStr)){
                    Long tmpLong =Long.parseLong(paraValueStr);
                    if(tmpLong<Integer.MAX_VALUE){
                        paraValue = tmpLong.intValue();
                    }else{
                        paraValue = tmpLong;
                    }
                }else if(paraValueStr.contains(CoreConstant.STRING_DOT)&&paraValueStr.split(CoreConstant.NUMBER_DOT).length==2){
                    if(StringUtils.isNumeric(paraValueStr.split(CoreConstant.NUMBER_DOT)[0])
                            &&StringUtils.isNumeric(paraValueStr.split(CoreConstant.NUMBER_DOT)[1])){
                        Double paraValueDouble = Double.parseDouble(paraValueStr);
                        paraValue = paraValueDouble.intValue();
                    }else{
                        paraValue = paraValueStr;
                    }
                }else{
                    paraValue = paraValueStr;
                }
                return paraValue;
            }else{
                LOG.warn("getParaError error ",paraConfigStr);
                sendNoticeMsg(new HashMap(){{
                    put("type", CoreConstant.JOB_NOTICE_TYPE_INIT_ERROR);
                    put("msg","请求 PARA config 异常，任务初始化失败,json=>"+paraConfigStr);
                }});
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,"请求 PARA config 异常，任务初始化失败,json=>"+paraConfigStr);
            }
        }catch (Exception ex){

            LOG.error("getParaError error ",ex);
            sendNoticeMsg(new HashMap(){{
                put("type", CoreConstant.JOB_NOTICE_TYPE_INIT_ERROR);
                put("msg","请求 PARA config 异常，任务初始化失败");
            }});
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,"请求 PARA config 异常，任务初始化失败");
        }
    }

    private Configuration fillPluginConfigWithNacos(Configuration pluginConfig){
        String pluginConfigNacos = pluginConfig.getString(CoreConstant.NACOS_CONFIG);
        if(StringUtils.isNotBlank(pluginConfigNacos) && pluginConfigNacos.startsWith("[") && pluginConfigNacos.endsWith("]")){
            String nacosDataId = pluginConfigNacos.substring(pluginConfigNacos.indexOf("[")+1,pluginConfigNacos.lastIndexOf("]"));
            String url = MessageFormat.format(this.coreConfigServerUrl,nacosDataId);
            LOG.info("fillPluginConfigWithNacosUrl=>[{}]",url);

            Request request = new Request.Builder()
                    .url(url)
                    .build();
            try (Response response = okHttpClient.newCall(request).execute()) {
                String resStr = response.body().string();
                LOG.info("fillPluginConfigWithNacosResStr=>[{}]",resStr);
                Map<String,Object> resMap = readGson.fromJson(resStr,Map.class);
                resMap.keySet().stream().forEach(key->{
                    pluginConfig.set(key,resMap.get(key));
                });
            }catch (Exception ex){
                LOG.error("get NACOS PLUGIN CONFIG error ",ex.getMessage());
                sendNoticeMsg(new HashMap(){{
                    put("type", CoreConstant.JOB_NOTICE_TYPE_INIT_ERROR);
                    put("msg","请求nacos获取"+nacosDataId+"异常，任务初始化失败");
                }});
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,"请求nacos获取"+nacosDataId+"异常，任务初始化失败");
            }
        }
        return pluginConfig;
    }

}
