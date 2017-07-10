package com.lm.kryo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

public class MyRegistrator implements KryoRegistrator {

	@Override
	public void registerClasses(Kryo arg0) {
		arg0.register(ConsumerRecord.class);
	}

}
