package com.sfconnector.bulkapisource.connector;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import static com.sfconnector.bulkapisource.connector.BulkApiSourceConnectorConfig.*;

import com.sfconnector.bulkapisource.response.CreateJobResponse;
import com.sfconnector.bulkapisource.sfdc.Bulk2Client;
import com.sfconnector.bulkapisource.sfdc.Bulk2ClientBuilder;
import com.sfconnector.bulkapisource.type.OperationEnum;

public class BulkApiSourceTask extends SourceTask {

    private static Logger log = LoggerFactory.getLogger(BulkApiSourceTask.class);

    private BulkApiSourceConnectorConfig config;
    private int monitorThreadTimeout;
    private List<String> sources;

    private String baseUrl;
    private String username;
    private String password;
    private String sfdc_key;
    private String sfdc_secret;
    private String sfdc_object;
    private String sfdc_query;
    private String topic;
    private String jobId;
    private String sfLocator;

    private Bulk2Client client;


    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new BulkApiSourceConnectorConfig(properties);
        monitorThreadTimeout = config.getInt(MONITOR_THREAD_TIMEOUT_CONFIG);
        
        String sourcesStr = properties.get("sources");
        sources = Arrays.asList(sourcesStr.split(","));

        setupTaskConfig(properties);
        

        // try{
        // 
        // createQueryJob();
        // }catch(Exception e){
        //     log.error("Exception occured during connection.");
        //     log.info(e.getMessage());
        //     log.info(e.getStackTrace().toString());
        // }


    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(monitorThreadTimeout / 2);
        List<SourceRecord> records = new ArrayList<>();
        for (String source : sources) {
            log.info("Polling data from the source '" + source + "'");
            records.add(new SourceRecord(
                buildSourcePartition(),
                buildSourceOffset(jobId, sfLocator),
                source, null, null, null, Schema.BYTES_SCHEMA,
                String.format("Data from %s", source).getBytes()));
        }
        
        return records;

    }

    @Override
    public void stop() {
    }


    private String createQueryJob(Bulk2Client client) throws InterruptedException{
        String jId = null;
        CreateJobResponse createJobResponse = client.createQuery(null, sfdc_query);
        jId = createJobResponse.getId();
        log.info("***********Job ID***- '" + jId + "'");

        return jId;
        // jobId = jId;
        // Thread.sleep(monitorThreadTimeout);
        // BufferedReader reader = new BufferedReader(client.getQueryResults(jobId, "9", ""));
        
        // log.info("Polling data from SF");
        // reader.lines().forEach(s -> log.info(s));

    }
    // private List<SourceRecord> getsObjectAsSourceRecords() {

    // }
    private Map<String, Object> buildSourcePartition(){
        
        Map<String, Object> map = new HashMap<>();
        map.put(SFDC_OBJECT_CONFIG, sfdc_object);
        // Some other fields needs to be added to differentiate the orgs.
        // map.put(USERNAME_CONFIG, username); modification required, uncomment may not work
        return map;
    }

    private Map<String, Object> buildSourceOffset(String jobId, String sfLocator){
        Map<String, Object> map = new HashMap<>();
        map.put("JOB_ID", jobId);
        map.put("SF_LOCATOR", sfLocator);
        return map;
    }

    private void setupTaskConfig(Map<String, String> props){
        config = new BulkApiSourceConnectorConfig(props);
        //baseUrl = config.getString(SFDC_URL_CONFIG);
        username = config.getString(USERNAME_CONFIG);
        password = config.getString(PASSWORD_CONFIG);
        sfdc_key = config.getString(SFDC_KEY_CONFIG);
        sfdc_secret = config.getString(SFDC_SECRET_CONFIG);
        sfdc_object = config.getString(SFDC_OBJECT_CONFIG);
        sfdc_query = config.getString(SFDC_QUERY_CONFIG);
        topic = config.getString(TOPIC_CONFIG);

        log.info("-------> setupTaskConfig config sfdc_key- '" + sfdc_key + "'");
    }

    private Bulk2Client buildApiClient() throws IOException{

        log.info("-------> setupTaskConfig config sfdc_key- '" + sfdc_key + "'");
        client = new Bulk2ClientBuilder().
                withPassword(sfdc_key, sfdc_secret, username, password)
                .build();
        
        log.info("Succesfully logged in....");

        return client;

    }

    private void initLastVars(){

        // Map<String, Object> persistedMap = null;

        // if(context !=null && context.offsetStorageReader() !=null ){
        //     persistedMap = context.offsetStorageReader().offset(buildSourcePartition());
        // }
        // log.info("Trying to get persistedMap.");
        // if(persistedMap !=null ){
        //     log.info("$$$$$$$$$$$$$$$$$$$$$$$$persistedMap.");
        // }

        Map<String, Object> lastSourceOffset = null;

        if(context !=null && context.offsetStorageReader() != null){
            lastSourceOffset = context.offsetStorageReader().offset(buildSourcePartition());
        }

        if(lastSourceOffset == null){
            try{
                Bulk2Client client = buildApiClient();
                jobId = createQueryJob(client);
                sfLocator = "-1";
                //processQueryResult(client, jobId, sfLocator);
            }catch(Exception e){}
            
        } else{
            try{
                Bulk2Client client = buildApiClient();

                Object jId = lastSourceOffset.get("JOB_ID");

                Object sLoc = lastSourceOffset.get("SF_LOCATOR");
                if(jId != null && (jId instanceof String)){
                    jobId = String.valueOf(jId);
                }
                if(sLoc != null && (sLoc instanceof String)){
                    sfLocator = String.valueOf(sLoc);
                }

                //processQueryResult(client, jobId, sfLocator);
            }catch(Exception e){}
        }
    }

    private SourceRecord generateSourceRecord(){

    }
}
