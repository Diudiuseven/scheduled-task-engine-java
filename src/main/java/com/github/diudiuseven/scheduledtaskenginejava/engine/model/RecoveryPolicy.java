package com.github.diudiuseven.scheduledtaskenginejava.engine.model;

/**
 * 重启后对“在途（RUNNING）”执行的恢复策略。
 */
public enum RecoveryPolicy {
    /** 视为一次失败尝试：仍有重试次数则退避重试，否则置为 FAILED */
    RETRY,
    /** 直接置为 FAILED */
    FAIL
}
