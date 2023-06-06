package com.jm.digital.intelligence.engine.model;

import java.io.Serializable;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/5 17:12
 */
public class ResResult<T> implements Serializable {
    private boolean success;
    private int code;
    private String message;
    private T data;
    private String detailMessage;

    public ResResult() {
    }

    public ResResult(T data) {
        this.success = true;
        this.code = 200;
        this.message = "调用成功";
        this.data = data;
    }

    public ResResult(String message, T data) {
        this.success = true;
        this.code = 200;
        this.message = message;
        this.data = data;
    }

    public ResResult(int code, String message, T data) {
        this.success = true;
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public ResResult(int code, String message, T data, String detailMessage) {
        this.success = true;
        this.code = code;
        this.message = message;
        this.data = data;
        this.detailMessage = detailMessage;
    }

    public static <T> ResResult<T> success() {
        return new ResResult((Object)null);
    }

    public static <T> ResResult<T> success(String message, T value) {
        return new ResResult((Object)null);
    }

    public static <T> ResResult<T> success(T value) {
        return new ResResult(value);
    }

    public static <T> ResResult<T> error(String message) {
        return error(500, message);
    }

    public static <T> ResResult<T> error(String message, String exception) {
        return error(500, message, exception);
    }


    public static <T> ResResult<T> error(Integer code, String customMsg) {
        return new ResResult(code, customMsg, (Object)null);
    }

    private static <T> ResResult<T> error(Integer code, String customMsg, String exception) {
        return new ResResult(code, customMsg, (Object)null, exception);
    }

    public static <T> ResResult<T> error(int code, String customMsg) {
        return new ResResult(code, customMsg, (Object)null);
    }

    public static <T> ResResult<T> error(int code, String customMsg, String exception) {
        return new ResResult(code, customMsg, (Object)null, exception);
    }

    public void setResultCode(ResResult<?> other) {
        if (other != null) {
            this.setCode(other.getCode());
            this.setMessage(other.getMessage());
            this.setSuccess(other.isSuccess());
        }

    }

    public String toString() {
        return String.format("ResResult {code=%s,message=%s,data=%s}", this.code, this.message, this.data);
    }

    public boolean isSuccess() {
        return this.success;
    }

    public int getCode() {
        return this.code;
    }

    public String getMessage() {
        return this.message;
    }

    public T getData() {
        return this.data;
    }

    public String getDetailMessage() {
        return this.detailMessage;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setData(T data) {
        this.data = data;
    }

    public void setDetailMessage(String detailMessage) {
        this.detailMessage = detailMessage;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof ResResult)) {
            return false;
        } else {
            ResResult<?> other = (ResResult)o;
            if (!other.canEqual(this)) {
                return false;
            } else if (this.isSuccess() != other.isSuccess()) {
                return false;
            } else if (this.getCode() != other.getCode()) {
                return false;
            } else {
                label52: {
                    Object this$message = this.getMessage();
                    Object other$message = other.getMessage();
                    if (this$message == null) {
                        if (other$message == null) {
                            break label52;
                        }
                    } else if (this$message.equals(other$message)) {
                        break label52;
                    }

                    return false;
                }

                Object this$data = this.getData();
                Object other$data = other.getData();
                if (this$data == null) {
                    if (other$data != null) {
                        return false;
                    }
                } else if (!this$data.equals(other$data)) {
                    return false;
                }

                Object this$detailMessage = this.getDetailMessage();
                Object other$detailMessage = other.getDetailMessage();
                if (this$detailMessage == null) {
                    if (other$detailMessage != null) {
                        return false;
                    }
                } else if (!this$detailMessage.equals(other$detailMessage)) {
                    return false;
                }

                return true;
            }
        }
    }

    protected boolean canEqual(Object other) {
        return other instanceof ResResult;
    }

    public int hashCode() {
        int result = 1;
        result = result * 59 + (this.isSuccess() ? 79 : 97);
        result = result * 59 + this.getCode();
        Object $message = this.getMessage();
        result = result * 59 + ($message == null ? 43 : $message.hashCode());
        Object $data = this.getData();
        result = result * 59 + ($data == null ? 43 : $data.hashCode());
        Object $detailMessage = this.getDetailMessage();
        result = result * 59 + ($detailMessage == null ? 43 : $detailMessage.hashCode());
        return result;
    }
}

