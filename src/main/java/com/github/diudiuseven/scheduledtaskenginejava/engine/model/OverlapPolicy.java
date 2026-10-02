package com.github.diudiuseven.scheduledtaskenginejava.engine.model;

/**
 * 执行耗时超过周期（同一任务上一次执行未结束）时的处理策略。
 */
public enum OverlapPolicy {
    /** 跳过本次触发点，记录跳过原因 */
    SKIP,
    /** 排队等待，上一次执行结束后按顺序补执行（队列有界，溢出则跳过） */
    QUEUE
}
