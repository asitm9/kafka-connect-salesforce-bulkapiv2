package com.sfconnector.bulkapisource.connector;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import static com.sfconnector.bulkapisource.connector.BulkApiSourceConnectorConfig.*;

import com.sfconnector.bulkapisource.sfdc.Bulk2Client;
import com.sfconnector.bulkapisource.sfdc.Bulk2ClientBuilder;

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

        log.debug("Trying to get persistedMap.");
        Map<String, Object> persistedMap = null;

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(monitorThreadTimeout / 2);
        List<SourceRecord> records = new ArrayList<>();
        for (String source : sources) {
            log.info("Polling data from the source '" + source + "'");
            records.add(new SourceRecord(
                Collections.singletonMap("source", source),
                Collections.singletonMap("offset", 0),
                source, null, null, null, Schema.BYTES_SCHEMA,
                String.format("Data from %s", source).getBytes()));
        }
        return records;
    }

    @Override
    public void stop() {
    }



    private void setupTaskConfig(Map<String, String> props) {
        baseUrl = props.get("SFDC_URL_CONFIG");
        username = props.get("USERNAME_CONFIG");
        password = props.get("PASSWORD_CONFIG");
        sfdc_key = props.get("SFDC_KEY_CONFIG");
        sfdc_secret = props.get("SFDC_SECRET_CONFIG");
        sfdc_object = props.get("SFDC_OBJECT_CONFIG");
        sfdc_query = props.get("SFDC_QUERY_CONFIG");
        topic = props.get("TOPIC_CONFIG");
        try{
            client = new Bulk2ClientBuilder().
                withPassword(sfdc_key, sfdc_secret, username, password)
                .build();
        } catch(Exception e){
            log.error("Exception occured during connection.");
        }
    }
}
