package com.lm;

import com.lm.spark.SparkStreamingKafka;
import com.lm.utils.SpringUtils;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		SparkStreamingKafka sparkStreamingKafka =
				SpringUtils.getApplicationContext().getBean(SparkStreamingKafka.class);
		try {
			sparkStreamingKafka.processSparkStreaming();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
