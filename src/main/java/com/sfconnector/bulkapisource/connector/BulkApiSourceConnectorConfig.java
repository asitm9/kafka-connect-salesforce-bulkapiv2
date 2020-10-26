package com.sfconnector.bulkapisource.connector;


import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;



public class BulkApiSourceConnectorConfig extends AbstractConfig {

    public BulkApiSourceConnectorConfig(final Map<?, ?> originalProps) {
        super(CONFIG_DEF, originalProps);
    }

    public static final String SFDC_URL_CONFIG = "salesforce.instance";
    public static final String SFDC_URL_DOC = "Salesforce login URL";
    
    public static final String USERNAME_CONFIG = "salesforce.username";
    private static final String USERNAME_DOC = "The username to use with the API.";

    public static final String PASSWORD_CONFIG = "salesforce.password";
    private static final String PASSWORD_DOC = "The password to use with the API.";

    public static final String SFDC_KEY_CONFIG = "salesforce.key";
    private static final String SFDC_KEY_DOC = "The consumer key to use with the API.";

    public static final String SFDC_SECRET_CONFIG = "salesforce.secret";
    private static final String SFDC_SECRET_DOC = "The consumer secret to use with the API.";

    public static final String SFDC_OBJECT_CONFIG = "salesforce.object";
    private static final String SFDC_OBJECT_DOC = "The object to use with the API.";

    public static final String SFDC_QUERY_CONFIG = "salesforce.query";
    private static final String SFDC_QUERY_DOC = "The query to use with the API.";

    public static final String TOPIC_CONFIG = "kafka.topic";
    private static final String TOPIC_DOC = "Topic to publish SFDC data to.";



    public static final String MONITOR_THREAD_TIMEOUT_CONFIG = "monitor.thread.timeout";
    private static final String MONITOR_THREAD_TIMEOUT_DOC = "Timeout used by the monitoring thread";
    private static final int MONITOR_THREAD_TIMEOUT_DEFAULT = 10000;

    public static final ConfigDef CONFIG_DEF = createConfigDef();

    private static ConfigDef createConfigDef() {
        ConfigDef configDef = new ConfigDef();
        addParams(configDef);
        return configDef;
    }

    private static void addParams(final ConfigDef configDef) {
        configDef.define(
            USERNAME_CONFIG,
            Type.STRING,
            Importance.HIGH,
            USERNAME_DOC)
        .define(
            PASSWORD_CONFIG,
            Type.STRING,
            Importance.HIGH,
            PASSWORD_DOC)
        .define(
            SFDC_KEY_CONFIG,
            Type.STRING,
            Importance.HIGH,
            SFDC_KEY_DOC)
        .define(
            SFDC_SECRET_CONFIG,
            Type.STRING,
            Importance.HIGH,
            SFDC_SECRET_DOC)
        .define(
                SFDC_OBJECT_CONFIG,
                Type.STRING,
                Importance.HIGH,
                SFDC_OBJECT_DOC)
        .define(
                    SFDC_QUERY_CONFIG,
                    Type.STRING,
                    Importance.HIGH,
                    SFDC_QUERY_DOC)
        .define(
            TOPIC_CONFIG,
                Type.STRING,
                Importance.HIGH,
                TOPIC_DOC)
        .define(
            MONITOR_THREAD_TIMEOUT_CONFIG,
            Type.INT,
            MONITOR_THREAD_TIMEOUT_DEFAULT,
            Importance.LOW,
            MONITOR_THREAD_TIMEOUT_DOC);
    }

}