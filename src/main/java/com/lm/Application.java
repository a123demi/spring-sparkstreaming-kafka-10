package com.lm;

import com.lm.exception.MessageException;
import com.lm.spark.SparkStreamingKafka2;
import com.lm.utils.SpringUtils;

/**
 * Hello world!
 *
 */
public class Application {
	public static void main(String[] args) {
		SparkStreamingKafka2 sparkStreamingKafka =
				SpringUtils.getApplicationContext().getBean(SparkStreamingKafka2.class);
		try {
			sparkStreamingKafka.processSparkStreaming();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (MessageException e) {
			
			System.out.println(e.getMessage());
		}
	}
}
