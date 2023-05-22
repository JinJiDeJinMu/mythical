package com.jm.dispatch;

import com.jm.param.Parameters;
import com.jm.utils.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/15 20:43
 */
public abstract class AbstractCommandDispatch<P extends Parameters> extends AbstractDispatch<P> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCommandDispatch.class);
    protected FutureTask processLogFuture;
    protected Thread processLogThread;
    protected long timeout = 3600L;
    protected long killTimeout = 60L;
    protected long logTimeout = 10L;
    protected TimeUnit timeUnit = TimeUnit.SECONDS;
    private Process process;
    private Integer processId;
    private ProcessBuilder processBuilder;

    public AbstractCommandDispatch(String dispatchContext) {
        super(dispatchContext);
    }

    @Override
    protected void preRun() {
        LOG.info("pre run...........");
    }

    @Override
    protected void doRun() {
        LOG.info("do run...........");
        this.processBuilder = buildProcessBuilder();

        try {
            this.process = processBuilder.start();
            this.processId = SystemUtils.getProcessId(process);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

        // 异步监听日志
        processLogFuture = new FutureTask<>(this::monitorProcessLog, null);
        processLogThread = new Thread(processLogFuture, "process-log");
        processLogThread.setDaemon(true);
        processLogThread.start();

        // 等待进程执行结束
        waitForProcess();
    }

    @Override
    protected void postRun() {
        LOG.info("post run...........");
    }

    @Override
    public void cancel() {
        LOG.info("cancel..........");
        if(!softHardKill(processId)){
            throw new RuntimeException("取消失败");
        }
    }

    protected void waitForProcess() {
        try {
            //超时阻塞
            boolean isFinish = process.waitFor(timeout, timeUnit);
            if (!isFinish) {
                //超过超时时间限制,主动去kill
                boolean killSuccess = softHardKill(processId);
            } else {
                int exitValue = process.exitValue();
                if (!isSuccess(exitValue)) {
                    //判断是否是kill
                    if (isKilled(exitValue)) {
                        // 进程被kill
                        LOG.info("进程被kill");
                    }
                }
            }
        } catch (InterruptedException exception) {
            LOG.error("进程执行中断");
        } catch (Throwable e) {
            LOG.error("进程执行失败," + e.getMessage());
        }

        // 等待日志输出结束
        waitForProcessLogFinish();
    }

    protected ProcessBuilder buildProcessBuilder() {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(buildCommand());
        processBuilder.redirectErrorStream(true);

        return processBuilder;
    }

    /**
     * 具体类实现
     *
     * @return
     */
    protected abstract List<String> buildCommand();



    protected void monitorProcessLog() {
        try (InputStream inputStream = process.getInputStream();
             InputStreamReader in = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             BufferedReader inReader = new BufferedReader(in)) {
            String line;
            while ((line = inReader.readLine()) != null) {
                //todo 如果需要分析日志获取applicationId，可以在这里处理
                LOG.info(line);

                if (processLogThread.isInterrupted()) {
                    throw new InterruptedException();
                }
            }
        } catch (Throwable e) {
            LOG.error(e.getMessage());
        }
    }

    protected void waitForProcessLogFinish() {
        try {
            processLogFuture.get(logTimeout, timeUnit);
        } catch (Throwable e) {
            // 强制中断监听日志线程
            processLogThread.interrupt();
        }
    }

    private boolean softHardKill(int processId) {
        if (killProcess(processId, false, killTimeout, timeUnit)) {
            return true;
        }
        return killProcess(processId, true, killTimeout, timeUnit);
    }

    /**
     * 执行kill进程命令
     */
    private boolean killProcess(int processId, boolean force, long timeout, TimeUnit timeUnit) {
        String[] killCmd = SystemUtils.buildKillCmd(processId, force);
        try {
            Process killProcess = Runtime.getRuntime().exec(killCmd);
            if (killProcess.waitFor(timeout, timeUnit)) {
                int exitValue = killProcess.exitValue();
                LOG.info("kill进程退出值：" + exitValue);
            } else {
                LOG.info("kill进程超时");
            }
        } catch (Exception e) {
            LOG.error("kill进程异常,", e);
        }
        return false;
    }


    /**
     * 判断进程退出值是否为0
     */
    private boolean isSuccess(int exitValue) {
        return exitValue == 0;
    }

    /**
     * 判断进程推出值是否为 130 或 143
     */
    private boolean isKilled(int exitValue) {
        return exitValue == 130 || exitValue == 143;
    }
}
