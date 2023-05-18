package com.jm.dispatch;

import cn.hutool.json.JSONUtil;
import com.jm.param.Parameters;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/15 20:35
 */
public abstract class AbstractDispatch<P extends Parameters> implements Dispatch{


    protected P parameters;

    protected DispatchContext dispatchContext;


    public AbstractDispatch(String dispatchContext){
        this.dispatchContext = JSONUtil.toBean(dispatchContext,DispatchContext.class);
        buildParameters();
    }

    protected void buildParameters() {
        this.parameters = JSONUtil.toBean(dispatchContext.getDispatchParameters(), getParametersClass());
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
