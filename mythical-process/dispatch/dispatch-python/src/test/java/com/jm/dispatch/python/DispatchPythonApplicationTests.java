package com.jm.dispatch.python;

import cn.hutool.json.JSONUtil;
import com.jm.dispatch.DispatchContext;
import com.jm.dispatch.python.dispatch.PythonDispatch;
import com.jm.dispatch.python.param.PythonParameters;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class DispatchPythonApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    void testRun(){
        DispatchContext dispatchContext = new DispatchContext();

        PythonParameters pythonParameters = new PythonParameters();
        pythonParameters.setCode("print(\"test python dispatch\")");
        pythonParameters.setEnvParameters("{\"PYTHON_HOME\":\"python3\"}");

        dispatchContext.setDispatchType("python");
        dispatchContext.setExecutePath("/Users/jinmu/Downloads");
        dispatchContext.setUid("123456");
        dispatchContext.setDispatchParameters(JSONUtil.toJsonStr(pythonParameters));

        PythonDispatch pythonDispatch = new PythonDispatch(JSONUtil.toJsonStr(dispatchContext));

        pythonDispatch.run();
    }
}
