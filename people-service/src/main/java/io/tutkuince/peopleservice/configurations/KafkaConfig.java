package io.tutkuince.peopleservice.configurations;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Configuration
public class KafkaConfig {

    static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${spring.kafka.producer.bootstrap-servers}")
    String bootstrapServers;

    @Value("${topics.people-basic.name}")
    String topicName;

    @Value("${topics.people-basic.partitions}")
    int topicPartitions;

    @Value("${topics.people-basic.replicas}")
    int topicReplicas;

    @Bean
    public NewTopic peopleBasicTopic() {
        return TopicBuilder.name(topicName)
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .build();
    }

    @Bean
    public NewTopic peopleBasicShortTopic() {
        return TopicBuilder.name(topicName + "-short")
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .config(TopicConfig.RETENTION_MS_CONFIG, "360000")
                .build();
    }

    @PostConstruct
    public void changePeopleBasicTopicRetention() throws ExecutionException, InterruptedException {
        // create a connection with configs to boostrap server
        Map<String, Object> connectionConfigs = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        );

        try(var admin = AdminClient.create(connectionConfigs)) {
            // create a config resource to fetch topics configs
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

            // filter down to specific topic (retention.ms)
            ConfigEntry topicConfigEntry = admin.describeConfigs(Collections.singleton(configResource))
                    .all().get().entrySet().stream()
                    .findFirst().get().getValue().entries().stream()
                    .filter(configEntry -> configEntry.name().equals(TopicConfig.RETENTION_MS_CONFIG))
                    .findFirst().get();

            // check if the config is 360,000 if not update to 1 hour
            if (!topicConfigEntry.value().equals("360000")) {
                // create a config entry and a alter config op to specify what config to change (retention.ms)
                Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = getConfigResourceCollectionMap(configResource);
                admin.incrementalAlterConfigs(alterConfigs).all().get();
                logger.info("Updated topic retention for " + topicName);
            }
        }


    }

    private static Map<ConfigResource, Collection<AlterConfigOp>> getConfigResourceCollectionMap(ConfigResource configResource) {
        var alterConfigEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "360000");
        var alterOp = new AlterConfigOp(alterConfigEntry, AlterConfigOp.OpType.SET);

        // use admin to exec the alter operation
        Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Map.of(
                configResource, Collections.singletonList(alterOp)
        );
        return alterConfigs;
    }
}
