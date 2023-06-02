package com.jm.dispatch.seatunnel.runner;

import com.jm.dispatch.AbstractCommandDispatch;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/15 20:46
 */
@Component
public class SeaTunnelRunner extends AbstractCommandDispatch {


    public SeaTunnelRunner(String dispatchContext) {
        super(dispatchContext);
    }

    @Override
    protected List<String> buildCommand() {
        return null;
    }

    @Override
    protected Class getParametersClass() {
        return null;
    }


    @Override
    public void cancel() {

    }
}
