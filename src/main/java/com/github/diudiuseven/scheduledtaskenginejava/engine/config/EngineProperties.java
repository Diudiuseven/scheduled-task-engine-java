package com.github.diudiuseven.scheduledtaskenginejava.engine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 引擎配置项（前缀 engine）。
 */
@ConfigurationProperties(prefix = "engine")
public class EngineProperties {

    /** 工作线程数。 */
    private int workerPoolSize = 4;

    /** 全局限速：每秒允许开始的执行数；<=0 表示不限。 */
    private double rateLimit = 10.0;

    /** 执行队列容量（有界）。 */
    private int queueCapacity = 200;

    /** 调度轮询间隔。 */
    private Duration tickInterval = Duration.ofMillis(200);

    /** 排队/并发等待超时。 */
    private Duration acquireTimeout = Duration.ofSeconds(5);

    /** 默认补跑窗口。 */
    private Duration defaultCatchUpWindow = Duration.ofHours(1);

    /** 默认重试次数（含首次）。 */
    private int defaultMaxAttempts = 3;

    /** 默认退避基数。 */
    private Duration defaultBackoffBase = Duration.ofSeconds(1);

    /** 默认退避上限。 */
    private Duration defaultBackoffMax = Duration.ofSeconds(30);

    /** 默认退避因子。 */
    private double defaultBackoffFactor = 2.0;

    /** 默认抖动比例。 */
    private double defaultJitterRatio = 0.2;

    /** H2 数据库文件路径。 */
    private String dbPath = "./data/scheduled-task-engine";

    public int getWorkerPoolSize() { return workerPoolSize; }
    public void setWorkerPoolSize(int workerPoolSize) { this.workerPoolSize = workerPoolSize; }

    public double getRateLimit() { return rateLimit; }
    public void setRateLimit(double rateLimit) { this.rateLimit = rateLimit; }

    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }

    public Duration getTickInterval() { return tickInterval; }
    public void setTickInterval(Duration tickInterval) { this.tickInterval = tickInterval; }

    public Duration getAcquireTimeout() { return acquireTimeout; }
    public void setAcquireTimeout(Duration acquireTimeout) { this.acquireTimeout = acquireTimeout; }

    public Duration getDefaultCatchUpWindow() { return defaultCatchUpWindow; }
    public void setDefaultCatchUpWindow(Duration defaultCatchUpWindow) { this.defaultCatchUpWindow = defaultCatchUpWindow; }

    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int defaultMaxAttempts) { this.defaultMaxAttempts = defaultMaxAttempts; }

    public Duration getDefaultBackoffBase() { return defaultBackoffBase; }
    public void setDefaultBackoffBase(Duration defaultBackoffBase) { this.defaultBackoffBase = defaultBackoffBase; }

    public Duration getDefaultBackoffMax() { return defaultBackoffMax; }
    public void setDefaultBackoffMax(Duration defaultBackoffMax) { this.defaultBackoffMax = defaultBackoffMax; }

    public double getDefaultBackoffFactor() { return defaultBackoffFactor; }
    public void setDefaultBackoffFactor(double defaultBackoffFactor) { this.defaultBackoffFactor = defaultBackoffFactor; }

    public double getDefaultJitterRatio() { return defaultJitterRatio; }
    public void setDefaultJitterRatio(double defaultJitterRatio) { this.defaultJitterRatio = defaultJitterRatio; }

    public String getDbPath() { return dbPath; }
    public void setDbPath(String dbPath) { this.dbPath = dbPath; }
}
