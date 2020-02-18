package org.embulk.output.bigquery_java;

import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import static com.google.cloud.bigquery.JobStatus.State.DONE;

public class BigqueryJobWaiter {
    private final Logger logger = LoggerFactory.getLogger(BigqueryJobWaiter.class);
    private Job job;
    private Job completedJob;
    private Date now;
    private Date started;
    private long elapsed;
    private JobStatus.State jobState;
    private PluginTask task;
    private BigqueryClient client;
    private JobStatistics jobStatistics;

    public BigqueryJobWaiter(PluginTask task, BigqueryClient client, Job job){
        this.task = task;
        this.client = client;
        this.job = job;
    }

    public JobStatistics waitFor(String kind){
        this.started = new Date();

        while (true) {
            completedJob = this.client.getJob(job.getJobId());
            jobState = completedJob.getStatus().getState();
            now = new Date();
            elapsed = (now.getTime() - started.getTime()) / 1000;
            if (jobState.equals(DONE)) {
                logger.info(String.format("embulk-output-bigquery: %s job completed... ", kind));
                logger.info(String.format("job_id:%s elapsed_time %d sec status[DONE]", completedJob.getJobId().getJob(), elapsed));
                break;
            } else if (elapsed > this.task.getJobStatusMaxPollingTime()) {
                logger.info(String.format("embulk-output-bigquery: %s job checking... ", kind));
                logger.info(String.format("job_id[%s] elapsed_time %d sec status[TIMEOUT]", completedJob.getJobId().getJob(), elapsed));
                break;
            } else {
                logger.info(String.format("embulk-output-bigquery: %s job checking... ", kind));
                logger.info(String.format("job_id[%s] elapsed_time %d sec status[%s]",
                        completedJob.getJobId().getJob(), elapsed, jobState.toString()));
                try {
                    Thread.sleep(this.task.getJobStatusPollingInterval() * 1000);
                } catch (InterruptedException e){
                    logger.info(e.getLocalizedMessage());
                    throw new RuntimeException(e);
                }
            }
        }

        if (completedJob.getStatus().getError() != null){
            logger.info(String.format("embulk-output-bigquery: job_id[%s] elapsed_time %d sec status[%s]",
                    completedJob.getJobId().getJob(), elapsed, jobState.toString()));
            logger.info(String.format("embulk-output-bigquery: %s job errors... job_id:[%s] errors:%s",
                    kind, completedJob.getJobId().getJob(), completedJob.getStatus().getError().getMessage()));
        }

        jobStatistics = completedJob.getStatistics();
        logger.info(String.format("embulk-output-bigquery: %s job response... job_id:[%s] response.statistics:%s",
                kind, completedJob.getJobId().getJob(), jobStatistics.toString()));

        return jobStatistics;
    }
}
