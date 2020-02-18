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
                logger.info("embulk-output-bigquery: {} job completed... ", kind);
                logger.info("job_id:{} elapsed_time {} sec status[DONE]", completedJob.getJobId().getJob(), elapsed);
                break;
            } else if (elapsed > this.task.getJobStatusMaxPollingTime()) {
                logger.info("embulk-output-bigquery: {} job checking... ", kind);
                logger.info("job_id[{}] elapsed_time {} sec status[TIMEOUT]", completedJob.getJobId().getJob(), elapsed);
                break;
            } else {
                logger.info("embulk-output-bigquery: {} job checking... ", kind);
                logger.info("job_id[{}] elapsed_time {} sec status[{}]",
                        completedJob.getJobId().getJob(), elapsed, jobState.toString());
                try {
                    Thread.sleep(this.task.getJobStatusPollingInterval() * 1000);
                } catch (InterruptedException e){
                    logger.info(e.getLocalizedMessage());
                    throw new RuntimeException(e);
                }
            }
        }

        if (completedJob.getStatus().getError() != null){
            logger.info("embulk-output-bigquery: job_id[{}] elapsed_time {} sec status[{}]",
                    completedJob.getJobId().getJob(), elapsed, jobState.toString());
            logger.info("embulk-output-bigquery: {} job errors... job_id:[{}] errors:{}",
                    kind, completedJob.getJobId().getJob(), completedJob.getStatus().getError().getMessage());
        }

        jobStatistics = completedJob.getStatistics();
        logger.info("embulk-output-bigquery: {} job response... job_id:[{}] response.statistics:{}",
                kind, completedJob.getJobId().getJob(), jobStatistics.toString());

        return jobStatistics;
    }
}
