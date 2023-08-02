package com.sai.springquartz.scheduler;

import javax.annotation.PostConstruct;

import java.util.Map;

import com.sai.springquartz.config.AutoWiringSpringBeanJobFactory;
import org.quartz.JobDetail;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SimpleTriggerFactoryBean;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

@Configuration
@EnableAutoConfiguration
@ConditionalOnExpression("'${using.spring.schedulerFactory}'=='true'")
public class SpringQrtzScheduler {

    Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ApplicationContext applicationContext;


    @Bean
    public SpringBeanJobFactory springBeanJobFactory() {
        AutoWiringSpringBeanJobFactory jobFactory = new AutoWiringSpringBeanJobFactory();
        logger.debug("Configuring Job factory");

        jobFactory.setApplicationContext(applicationContext);
        return jobFactory;
    }

    @Bean
    public SchedulerFactoryBean scheduler(Map<String, JobDetail> jobMap, Map<String, Trigger> trigger) {

        SchedulerFactoryBean schedulerFactory = new SchedulerFactoryBean();
        schedulerFactory.setConfigLocation(new ClassPathResource("quartz.yml"));

        logger.debug("Setting the Scheduler up");
        schedulerFactory.setJobFactory(springBeanJobFactory());
        schedulerFactory.setJobDetails(jobMap.values().toArray(new JobDetail[0]));
        schedulerFactory.setTriggers(trigger.values().toArray(new Trigger[0]));

        // Comment the following line to use the default Quartz job store.
       // schedulerFactory.setDataSource(quartzDataSource);

        return schedulerFactory;



    }


    @Bean("startjob")
    public JobDetailFactoryBean startJobDetail() {

        JobDetailFactoryBean jobDetailFactory = new JobDetailFactoryBean();
        jobDetailFactory.setJobClass(StartKafkaListenerJob.class);
        jobDetailFactory.setName("Qrtz_Job_Detail-start");
        jobDetailFactory.setDescription("Invoke StartKafkaListenerJob..");
        jobDetailFactory.setDurability(true);
        return jobDetailFactory;
    }

    @Bean("stopJob")
    public JobDetailFactoryBean stopJobDetail() {

        JobDetailFactoryBean jobDetailFactory = new JobDetailFactoryBean();
        jobDetailFactory.setJobClass(SopKafkaListenerJob.class);
        jobDetailFactory.setName("Qrtz_Job_Detail-stop");
        jobDetailFactory.setDescription("Invoke SopKafkaListenerJob...");
        jobDetailFactory.setDurability(true);
        return jobDetailFactory;
    }

   @Bean
    public CronTriggerFactoryBean stopTrigger(@Qualifier("stopJob") JobDetail job) {

       CronTriggerFactoryBean trigger = new CronTriggerFactoryBean();
        trigger.setJobDetail(job);

        //int frequencyInSec = 10;
        logger.info("Configuring trigger to fire every {} seconds", 120);

        //trigger.setRepeatInterval(frequencyInSec * 1000);
        //trigger.setRepeatCount(SimpleTrigger.REPEAT_INDEFINITELY);
       //every one 2minute will run
       trigger.setCronExpression("0 0/2 * ? * *");
       trigger.setName("Qrtz_Trigger-1");
        return trigger;
    }

    @Bean
    public CronTriggerFactoryBean trigger(@Qualifier("startjob")  JobDetail job) {


        CronTriggerFactoryBean trigger = new CronTriggerFactoryBean();
        trigger.setJobDetail(job);

        //int frequencyInSec = 10;
        logger.info("Configuring trigger to fire every {} seconds", 60);


        //every one minute will run
        trigger.setCronExpression("0 0/1 * ? * *");
        trigger.setName("Qrtz_Trigger");
        return trigger;
    }

   /* @Bean
    @QuartzDataSource
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource quartzDataSource() {
        return DataSourceBuilder.create().build();
    }*/

}
