package com.alibaba.datax.core.taskgroup.runner;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alipay.common.tracer.core.context.trace.SofaTraceContext;
import com.alipay.common.tracer.core.holder.SofaTraceContextHolder;
import com.alipay.common.tracer.core.span.SofaTracerSpan;
import org.slf4j.MDC;

public class TaskGroupContainerRunner implements Runnable {

	private TaskGroupContainer taskGroupContainer;

	private State state;

	public TaskGroupContainerRunner(TaskGroupContainer taskGroup) {
		this.taskGroupContainer = taskGroup;
		this.state = State.SUCCEEDED;
	}

	@Override
	public void run() {
		try {
			MDC.remove("DATAX-JOBID");
			MDC.remove("DATAX-TASKID");
			MDC.remove("DATAX-STATUS");
			SofaTraceContext sofaTraceContext = SofaTraceContextHolder.getSofaTraceContext();
			SofaTracerSpan sofaTracerSpan = sofaTraceContext.getCurrentSpan();
			String jobId = sofaTracerSpan.getBaggageItem("DATAX-JOBID");
			String taskId = sofaTracerSpan.getBaggageItem("DATAX-TASKID");
			MDC.put("DATAX-JOBID",jobId);
			MDC.put("DATAX-TASKID",taskId+"");
			MDC.put("DATAX-STATUS","RUNNING");

            Thread.currentThread().setName(
                    String.format("taskGroup-%d", this.taskGroupContainer.getTaskGroupId()));
            this.taskGroupContainer.start();
			this.state = State.SUCCEEDED;
		} catch (Throwable e) {
			this.state = State.FAILED;
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		}
	}

	public TaskGroupContainer getTaskGroupContainer() {
		return taskGroupContainer;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}
}
