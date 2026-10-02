package com.github.diudiuseven.scheduledtaskenginejava.engine;

/**
 * 任务执行处理器。实现必须可被中断：当执行被取消时，工作线程会收到中断信号，
 * 处理器应尽快退出且不留下半完成状态（建议处理器自身保证幂等性）。
 */
@FunctionalInterface
public interface TaskHandler {

    /**
     * 执行一次触发。
     *
     * @param ctx 执行上下文（任务、触发时刻、第几次尝试）
     * @throws Exception 任何异常都将触发退避重试
     */
    void execute(ExecutionContext ctx) throws Exception;
}
