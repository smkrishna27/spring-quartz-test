package com.sai.springquartz.scheduler;

import com.sai.springquartz.service.KafkaStartStopService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StartKafkaListenerJob implements Job {

    Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private KafkaStartStopService jobService;

    public void execute(JobExecutionContext context) throws JobExecutionException {

        logger.info("Job ** {} ** fired @ {}", context.getJobDetail().getKey().getName(), context.getFireTime());

        jobService.startJob();

        logger.info("Next start job scheduled @ {}", context.getNextFireTime());
    }
}
