package com.jm.dispatch;

import cn.hutool.json.JSONUtil;
import com.jm.dispatch.log.HdfsLogStorage;
import com.jm.dispatch.log.LocalLogStorage;
import com.jm.dispatch.log.LogStorage;
import com.jm.param.Parameters;

import java.util.Optional;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/15 20:35
 */
public abstract class AbstractDispatch<P extends Parameters> implements Dispatch{

    protected P parameters;
    protected DispatchContext dispatchContext;
    protected LogStorage logStorage;

    public AbstractDispatch(String dispatchContext){
        this.dispatchContext = JSONUtil.toBean(dispatchContext,DispatchContext.class);
        buildParameters();
        selectLogStorage();
    }

    protected void buildParameters() {
        this.parameters = JSONUtil.toBean(dispatchContext.getDispatchParameters(), getParametersClass());
    }

    protected void selectLogStorage(){
        String logStorageType = Optional.ofNullable(this.dispatchContext.getLogStorageType()).orElse("hdfs");
        this.logStorage = logStorageType.equalsIgnoreCase("hdfs")?new HdfsLogStorage():new LocalLogStorage();
    }

    protected abstract Class<P> getParametersClass();


    @Override
    public void run() {
        try {
            //---------------
            preRun();

            //---------------
            doRun();

            //--------------
            postRun();


        }catch (Exception e){
           e.printStackTrace();
        }finally {
            try {
                clear();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    protected abstract void preRun();

    protected abstract void doRun();

    protected abstract void postRun();

    protected abstract void clear();
}
