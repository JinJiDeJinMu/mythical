package com.nebula.hs.etl.config;

/**
 * @Author ChengJie
 * @Date 2023/6/28
 */
public class ExtraConfig {
    private String dataCountKafkaServers;

    private String dataCountTopicName;

    public String getDataCountKafkaServers() {
        return dataCountKafkaServers;
    }

    public void setDataCountKafkaServers(String dataCountKafkaServers) {
        this.dataCountKafkaServers = dataCountKafkaServers;
    }

    public String getDataCountTopicName() {
        return dataCountTopicName;
    }

    public void setDataCountTopicName(String dataCountTopicName) {
        this.dataCountTopicName = dataCountTopicName;
    }
}
