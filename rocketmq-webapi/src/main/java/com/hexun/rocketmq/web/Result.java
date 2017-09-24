package com.hexun.rocketmq.web;

public class Result {
    /**
     * code
     */
    private Integer code;
    /**
     * message
     */
    private String msg;
    /**
     * result data
     */
    private Object data;

    /**
     * 状态码
     *
     * @return code
     */
    public Integer getCode() {
        return code;
    }

    private void setCode(Integer code) {
        this.code = code;
    }

    /**
     * 消息
     *
     * @return msg
     */
    public String getMsg() {
        return msg;
    }

    private void setMsg(String msg) {
        this.msg = msg;
    }

    /**
     * 数据
     *
     * @return data
     */
    public Object getData() {
        return data;
    }

    private void setData(Object data) {
        this.data = data;
    }


    static Result success(Object data) {
        Result result = new Result();
        result.setCode(0);
        result.setMsg("");
        result.setData(data);
        return result;
    }

    static Result fail(Integer errorCode, String message) {
        Result result = new Result();
        result.setCode(errorCode);
        result.setMsg(message);
        return result;
    }
}
