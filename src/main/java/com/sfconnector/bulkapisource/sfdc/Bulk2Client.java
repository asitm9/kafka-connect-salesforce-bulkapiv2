package com.sfconnector.bulkapisource.sfdc;

import java.io.Reader;
import java.util.function.Consumer;

import com.sfconnector.bulkapisource.request.CloseOrAbortJobRequest;
import com.sfconnector.bulkapisource.request.CreateJobRequest;
import com.sfconnector.bulkapisource.request.GetAllJobsRequest;
import com.sfconnector.bulkapisource.response.CloseOrAbortJobResponse;
import com.sfconnector.bulkapisource.response.CreateJobResponse;
import com.sfconnector.bulkapisource.response.GetAllJobsResponse;
import com.sfconnector.bulkapisource.response.GetJobInfoResponse;
import com.sfconnector.bulkapisource.response.JobInfo;
import com.sfconnector.bulkapisource.type.JobStateEnum;
import com.sfconnector.bulkapisource.type.OperationEnum;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





public class Bulk2Client {

    private static final Logger log = LoggerFactory.getLogger(Bulk2Client.class);

    private static final String API_VERSION = "v49.0";

    private final RestRequester requester;

    private final String instanceUrl;

    public Bulk2Client(RestRequester requester, String instanceUrl) {
        this.instanceUrl = instanceUrl;
        this.requester = requester;
    }

    public CreateJobResponse createJob(String object, OperationEnum operation) {
        return createJob(object, operation, (request) -> {
        });
    }


    public CreateJobResponse createQuery(String object, String query) {
        return createQuery(object, query, (request) -> {
        });
    }

    public CreateJobResponse createQuery(String object, String query, Consumer<CreateJobRequest.Builder> requestBuilder) {
        String url = buildUrl("/services/data/vXX.X/jobs/query");

        CreateJobRequest.Builder builder = new CreateJobRequest.Builder(object, OperationEnum.QUERY);
        builder = builder.withQuery(query);
        requestBuilder.accept(builder);

        return requester.post(url, builder.build(), CreateJobResponse.class);
    }

    public CreateJobResponse createJob(String object, OperationEnum operation, Consumer<CreateJobRequest.Builder> requestBuilder) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest");

        CreateJobRequest.Builder builder = new CreateJobRequest.Builder(object, operation);
        requestBuilder.accept(builder);

        return requester.post(url, builder.build(), CreateJobResponse.class);
    }

    public CloseOrAbortJobResponse closeOrAbortJob(String jobId, JobStateEnum state) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId);

        CloseOrAbortJobRequest.Builder builder = new CloseOrAbortJobRequest.Builder(state);

        return requester.patch(url, builder.build(), CloseOrAbortJobResponse.class);
    }

    public void uploadJobData(String jobId, String csvContent) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId + "/batches");

        requester.putCsv(url, csvContent, Void.class);
    }

    public void deleteJob(String jobId) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId);

        requester.delete(url, null, Void.class);
    }

    public GetAllJobsResponse getAllJobs() {
        return getAllJobs(request -> {
        });
    }

    public GetAllJobsResponse getAllJobs(Consumer<GetAllJobsRequest.Builder> requestBuilder) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest");

        GetAllJobsRequest.Builder builder = new GetAllJobsRequest.Builder();
        requestBuilder.accept(builder);

        return requester.get(url, builder.buildParameters(), GetAllJobsResponse.class);
    }

    public GetJobInfoResponse getJobInfo(String jobId) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId);

        return requester.get(url, GetJobInfoResponse.class);
    }

    public Reader getJobSuccessfulRecordResults(String jobId) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId + "/successfulResults/");

        return requester.getCsv(url);
    }

    public Reader getQueryResults(String jobId, String maxRec, String locator) {
        String url = buildUrl("/services/data/vXX.X/jobs/query/" + jobId + "/results/?maxRecords=" + maxRec);

        return requester.getCsv(url);
    }

    public Reader getJobFailedRecordResults(String jobId) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId + "/failedResults/");

        return requester.getCsv(url);
    }

    public Reader getJobUnprocessedRecordResults(String jobId) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId + "/unprocessedrecords/");

        return requester.getCsv(url);
    }

    // alias

    public JobInfo closeJob(String jobId) {
        return closeOrAbortJob(jobId, JobStateEnum.UPLOAD_COMPLETE);
    }

    public JobInfo abortJob(String jobId) {
        return closeOrAbortJob(jobId, JobStateEnum.ABORTED);
    }

    private String buildUrl(String path) {
        boolean hasTrailingSlash = instanceUrl.endsWith("/");

        return instanceUrl + (hasTrailingSlash ? "/" : "") + path.replace("vXX.X", API_VERSION);
    }
}