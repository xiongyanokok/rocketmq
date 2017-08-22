package com.hexun.rocketmq.web;

public class Result {
    /**
     * code
     */
    Integer code;
    /**
     * message
     */
    String msg;
    /**
     * result data
     */
    Object data;

    /**
     * 状态码
     *
     * @return
     */
    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    /**
     * 消息
     *
     * @return
     */
    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    /**
     * 数据
     *
     * @return
     */
    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }


    public static Result success(Object data) {
        Result result = new Result();
        result.code = 0;
        result.msg = "";
        result.data = data;
        return result;
    }

    public static Result fail(Integer errorCode, String message) {
        Result result = new Result();
        result.code = errorCode;
        result.msg = message;
        return result;
    }
}
