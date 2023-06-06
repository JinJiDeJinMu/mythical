package com.jm.digital.gateway.model;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/3 14:44
 */
public class CustomResult {

    private String codeDesc;
    private Integer codeNum;
    private Boolean success;
    private Object value;
    private String errMsg;
    private String requestId;
    private Integer errCode;

    public CustomResult(String codeDesc, Integer codeNum, Boolean success, Object value) {
        this.codeDesc = codeDesc;
        this.codeNum = codeNum;
        this.success = success;
        this.value = value;
    }

    public String getCodeDesc() {
        return codeDesc;
    }

    public void setCodeDesc(String codeDesc) {
        this.codeDesc = codeDesc;
    }

    public Integer getCodeNum() {
        return codeNum;
    }

    public void setCodeNum(Integer codeNum) {
        this.codeNum = codeNum;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Integer getErrCode() {
        return errCode;
    }

    public void setErrCode(Integer errCode) {
        this.errCode = errCode;
    }
}
