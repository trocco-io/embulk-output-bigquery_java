package org.embulk.output.bigquery_java;

import static com.google.cloud.bigquery.JobStatus.State.DONE;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.exception.BigqueryBackendException;
import org.embulk.output.bigquery_java.exception.BigqueryException;
import org.embulk.output.bigquery_java.exception.BigqueryInternalException;
import org.embulk.output.bigquery_java.exception.BigqueryJobTimeoutException;
import org.embulk.output.bigquery_java.exception.BigqueryRateLimitExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public BigqueryJobWaiter(PluginTask task, BigqueryClient client, Job job) {
        this.task = task;
        this.client = client;
        this.job = job;
    }

    public JobStatistics waitFor(String kind) throws RuntimeException {
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
                throw new BigqueryJobTimeoutException(String.format("Time out job_id[%s] elapsed_time %d", completedJob.getJobId().getJob(), elapsed));
            } else {
                logger.info("embulk-output-bigquery: {} job checking... ", kind);
                logger.info("job_id[{}] elapsed_time {} sec status[{}]",
                        completedJob.getJobId().getJob(), elapsed, jobState.toString());
                try {
                    Thread.sleep(this.task.getJobStatusPollingInterval() * 1000);
                } catch (InterruptedException e) {
                    logger.info(e.getMessage());
                }
            }
        }

        /*
         * JobStatus.getError()
         * Returns the final error result of the job. If present, indicates that the job has completed and
         * was unsuccessful.
         *
         * JobStatus.getExecutionErrors()
         * Returns all errors encountered during the running of the job. Errors here do not necessarily
         * mean that the job has completed or was unsuccessful.
         *
         * https://cloud.google.com/bigquery/troubleshooting-errors
         */
        if (completedJob.getStatus().getError() != null) {
            String msg = String.format("failed during waiting a %s job get_job(%s, errors: %s)",
                    kind, completedJob.getJobId().getJob(), bigqueryErrorToString(completedJob));

            List<String> bigqueryErrors = completedJob.getStatus().getExecutionErrors()
                    .stream()
                    .map(BigQueryError::getReason)
                    .collect(Collectors.toList());
            if (bigqueryErrors.contains("backendError")) {
                throw new BigqueryBackendException(msg);
            } else if (bigqueryErrors.contains("internalError")) {
                throw new BigqueryInternalException(msg);
            } else if (bigqueryErrors.contains("rateLimitExceeded")) {
                throw new BigqueryRateLimitExceededException(msg);
            } else {
                logger.error("embulk-output-bigquery: {}", msg);
                throw new BigqueryException(msg);
            }
        }

        if (completedJob.getStatus().getExecutionErrors() != null) {
            logger.warn("embulk-output-bigquery: {} job errors... job_id:[{}] errors:{}",
                    kind, completedJob.getJobId().getJob(), bigqueryErrorToString(completedJob));
        }

        jobStatistics = completedJob.getStatistics();
        logger.info("embulk-output-bigquery: {} job response... job_id:[{}] response.statistics:{}",
                kind, completedJob.getJobId().getJob(), jobStatistics.toString());

        return jobStatistics;
    }


    private String bigqueryErrorToString(Job job) {
        return job.getStatus().getExecutionErrors().stream()
                .map(BigQueryError::toString)
                .collect(Collectors.joining(", "));
    }
}
