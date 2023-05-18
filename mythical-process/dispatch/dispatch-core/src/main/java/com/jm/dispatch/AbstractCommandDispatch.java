package com.jm.dispatch;

import com.jm.param.Parameters;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/15 20:43
 */
public abstract class AbstractCommandDispatch<P extends Parameters> extends AbstractDispatch<P>{
    public AbstractCommandDispatch(String dispatchContext) {
        super(dispatchContext);
    }
}
