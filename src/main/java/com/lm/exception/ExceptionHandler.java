package com.lm.exception;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Aspect
@Component
public class ExceptionHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionHandler.class);

	/**
	 * @within(org.springframework.stereotype.Service)，拦截带有 @Service 注解的类的所有方法
	 * @annotation(org.springframework.web.bind.annotation.RequestMapping)，
	 * 拦截带有@RquestMapping的注解方法
	 */
	@Pointcut("execution(public * *(..))")
	private void handlerPointcut() {
	}

	/**
	 * 拦截service层异常，记录异常日志，并设置对应的异常信息 目前只拦截Exception，是否要拦截Error需再做考虑
	 *
	 * @param e
	 *            异常对象
	 */
	@AfterThrowing(pointcut = "handlerPointcut()", throwing = "e")
	public void handle(JoinPoint point, Exception e) {
		String signature = point.getSignature().toString();
		String errorMsg = getMessage(signature) == null
				? (StringUtils.isEmpty(e.getMessage()) ? "服务异常" : e.getMessage()) : getMessage(signature);
		LOGGER.error(errorMsg);
		// throw new ServiceException(errorMsg, e);
	}

	/**
	 * 获取方法签名对应的提示消息
	 *
	 * @param signature
	 *            方法签名
	 * @return 提示消息
	 */
	private String getMessage(String signature) {
		return null;
	}
}
