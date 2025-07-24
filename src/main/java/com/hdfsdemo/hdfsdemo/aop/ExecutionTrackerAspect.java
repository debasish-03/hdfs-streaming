package com.hdfsdemo.hdfsdemo.aop;


import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class ExecutionTrackerAspect {

    private static final Logger logger = LoggerFactory.getLogger(ExecutionTrackerAspect.class);

    @Around("@annotation(com.hdfsdemo.hdfsdemo.aop.TrackExecution)")
    public Object trackExecutionTime(ProceedingJoinPoint joinPoint) throws Throwable {
        long start = System.currentTimeMillis();

        String methodName = joinPoint.getSignature().toShortString();
        logger.info("Starting execution of {}", methodName);

        try {
            Object result = joinPoint.proceed();
            long duration = System.currentTimeMillis() - start;
            logger.info("Completed execution of {} in {} ms", methodName, duration);
            return result;
        } catch (Throwable t) {
            long duration = System.currentTimeMillis() - start;
            logger.error("Execution of {} failed after {} ms with exception: {}", methodName, duration, t.getMessage(), t);
            throw t;
        }
    }
}

