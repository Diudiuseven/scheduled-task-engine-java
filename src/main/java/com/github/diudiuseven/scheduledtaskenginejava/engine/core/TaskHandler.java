package com.github.diudiuseven.scheduledtaskenginejava.engine.core;

import com.github.diudiuseven.scheduledtaskenginejava.engine.model.TaskExecution;

/**
 * 业务执行体。抛出异常视为执行失败，将进入重试流程。
 */
@FunctionalInterface
public interface TaskHandler {

    void execute(TaskExecution execution) throws Exception;
}
