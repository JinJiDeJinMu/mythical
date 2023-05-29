package com.jm.dispatch.log.model;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/26 15:45
 */
public class LogResult {

    private Long currentLine;

    private String message;

    public LogResult(Long currentLine,String message){
        this.currentLine = currentLine;
        this.message = message;
    }

    public void setCurrentLine(Long currentLine) {
        this.currentLine = currentLine;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getCurrentLine() {
        return currentLine;
    }

    public String getMessage() {
        return message;
    }
}
