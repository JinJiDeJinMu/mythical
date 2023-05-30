package com.jm.dispatch.python.dispatch;

import cn.hutool.json.JSONUtil;
import com.jm.dispatch.AbstractCommandDispatch;
import com.jm.dispatch.python.param.PythonParameters;
import com.jm.utils.SystemUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

import static com.jm.utils.Constant.SPACE_SIGN;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/30 11:02
 */
public class PythonDispatch extends AbstractCommandDispatch<PythonParameters> {

    private String pythonHome;
    private static final String PYTHON_HOME = "PYTHON_HOME";

    private List<File> resourceFileList;

    private File pythonCodeFile;

    public PythonDispatch(String dispatchContext) {
        super(dispatchContext);
    }

    private void checkPythonHome(String envParameters) {
        if(StringUtils.isBlank(envParameters) || !envParameters.contains(PYTHON_HOME)){
            this.pythonHome = System.getenv(PYTHON_HOME);
        }else {
            HashMap hashMap = JSONUtil.toBean(envParameters, HashMap.class);
            this.pythonHome = hashMap.get(PYTHON_HOME).toString();
        }
    }

    @Override
    protected void preRun() {
        super.preRun();
        checkPythonHome(parameters.getEnvParameters());
        //todo 下载资源文件到本地?
       if(CollectionUtils.isNotEmpty(parameters.getResourceInfoList())) {

       }
       buildPythonCommandFile();
    }

    @Override
    protected void postRun() {
        super.postRun();
        //todo 清理文件
    }

    @Override
    protected List<String> buildCommand() {
        List<String> cmdArgs = SystemUtils.buildCmdArgs();
        String command = buildPythonCommand();
        cmdArgs.add(command);
        return cmdArgs;
    }

    @Override
    protected Class getParametersClass() {
        return PythonParameters.class;
    }


    private String buildPythonCommand() {
        StringBuilder sb = new StringBuilder(pythonHome).append(SPACE_SIGN).append(pythonCodeFile.getAbsolutePath());
        if(StringUtils.isNotBlank(parameters.getRunParameters())){
            sb.append(SPACE_SIGN).append(parameters.getRunParameters());
        }
        if(CollectionUtils.isNotEmpty(resourceFileList)){
            resourceFileList.stream().forEach(e->sb.append(SPACE_SIGN).append(e));
        }
        return sb.toString();
    }

    private void buildPythonCommandFile() {
        String fileName = String.format("%s/%s.py", this.dispatchContext.getExecutePath() + "/code",dispatchContext.getUid());

        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        String code = buildPythonCode();
        try {
            FileUtils.writeStringToFile(file, code, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.pythonCodeFile = file;
    }

    private String buildPythonCode() {
        String code = parameters.getCode().replaceAll("\\r\\n", "\n");
        try {
            code = convertPythonScriptPlaceholders(code);
        } catch (StringIndexOutOfBoundsException e) {
            throw new RuntimeException(e.getMessage());
        }
        return code;
    }


    private static String convertPythonScriptPlaceholders(String rawScript) throws StringIndexOutOfBoundsException {
        int len = "${setShareVar(${".length();
        int scriptStart = 0;
        while ((scriptStart = rawScript.indexOf("${setShareVar(${", scriptStart)) != -1) {
            int start = -1;
            int end = rawScript.indexOf('}', scriptStart + len);
            String prop = rawScript.substring(scriptStart + len, end);
            start = rawScript.indexOf(',', end);
            end = rawScript.indexOf(')', start);

            String value = rawScript.substring(start + 1, end);

            start = rawScript.indexOf('}', start) + 1;
            end = rawScript.length();

            String replaceScript = String.format("print(\"${{setValue({},{})}}\".format(\"%s\",%s))", prop, value);
            rawScript = rawScript.substring(0, scriptStart) + replaceScript + rawScript.substring(start, end);
            scriptStart += replaceScript.length();
        }
        return rawScript;
    }
}
