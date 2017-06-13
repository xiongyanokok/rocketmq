package com.hexun.rocketmq;

import java.io.Serializable;

public interface MessageConsumer<E extends Serializable> {

	public void consume(E messageObject) throws Exception;

}