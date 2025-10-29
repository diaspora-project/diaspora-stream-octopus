#ifndef OCTOPUS_KAFKA_HELPER_H
#define OCTOPUS_KAFKA_HELPER_H

#include "octopus/KafkaConf.hpp"
#include <diaspora/Exception.hpp>
#include <uuid.h>
#include <librdkafka/rdkafka.h>
#include <string>
#include <memory>

namespace octopus {

/**
 * This function reads a full topic (used to read an info topic). The topic should
 * only have one partition, since consumption will stop at the first PARTITION_EOF
 * received.
 */
static inline std::vector<std::string> readFullTopic(std::string_view name, KafkaConf conf) {

    char errstr[512];

    uuid_t consumer_uuid;
    uuid_generate(consumer_uuid);
    char group_id[37] =  {0};
    uuid_unparse(consumer_uuid, group_id);
    conf["group.id"] = std::string{"info-consurmer-"} + group_id;
    conf["auto.offset.reset"] = "earliest";
    conf["topic.metadata.refresh.interval.ms"] = "10000";
    conf["enable.partition.eof"] = "true";

    auto kconf = conf.dup();

    auto consumer = rd_kafka_new(RD_KAFKA_CONSUMER, kconf, errstr, sizeof(errstr));
    if (!consumer) {
        rd_kafka_conf_destroy(kconf);
        throw diaspora::Exception{
            "Could not create rd_kafka_t instance: " + std::string{errstr}};
    }
    auto _consumer = std::shared_ptr<rd_kafka_t>{consumer, rd_kafka_destroy};

    // Create the topic object
    auto info_topic = rd_kafka_topic_new(consumer, name.data(), nullptr);
    if (!info_topic)
        throw diaspora::Exception{
            "Failed to create topic object: " + std::string{rd_kafka_err2str(rd_kafka_last_error())}};
    auto _info_topic = std::shared_ptr<rd_kafka_topic_t>{info_topic, rd_kafka_topic_destroy};

    // Create a topic partition list
    auto topic_partition_list = rd_kafka_topic_partition_list_new(1);
    if (!topic_partition_list)
        throw diaspora::Exception{
            "Failed to create topic partition list: " + std::string{rd_kafka_err2str(rd_kafka_last_error())}};
    auto _topic_partition_list = std::shared_ptr<rd_kafka_topic_partition_list_t>{
        topic_partition_list,
        rd_kafka_topic_partition_list_destroy};

    // Add the topic to the list
    rd_kafka_topic_partition_list_add(
        topic_partition_list, name.data(), RD_KAFKA_PARTITION_UA);

    // Subscribe to the topic
    rd_kafka_resp_err_t err = rd_kafka_subscribe(consumer, topic_partition_list);
    if (err)
        throw diaspora::Exception{
            "Failed to subscribe to topic: " + std::string{rd_kafka_err2str(err)}};

    std::vector<std::string> result;

    rd_kafka_message_t *message;
    while (true) {
        message = rd_kafka_consumer_poll(consumer, 1000);
        if (message) {
            auto _message = std::shared_ptr<rd_kafka_message_t>{message, rd_kafka_message_destroy};
            if (message->err) {
                if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    break;
                } else {
                    throw diaspora::Exception{
                        "Consumer error: " + std::string{rd_kafka_message_errstr(message)}};
                }
            } else {
                // Message received
                result.emplace_back(static_cast<char*>(message->payload), message->len);
            }
        } else {
            // Timeout, continue polling
            continue;
        }
    }

    rd_kafka_unsubscribe(consumer);

    return result;
}

/**
 * @brief Returns the last (high watermark) offset for a given topic.
 * This assumes the topic has exactly one partition (partition 0).
 */
static inline int64_t getNumEventsInPartition(KafkaConf kconf,
                                              const std::string& topic,
                                              int timeout_ms = 5000)
{
    char errstr[512];

    // Create a temporary Kafka handle (producer works fine)
    rd_kafka_conf_t *conf = kconf.dup();
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{std::string("Failed to create Kafka handle: ") + errstr};
    }

    int64_t low, high;
    rd_kafka_resp_err_t err = rd_kafka_query_watermark_offsets(
        rk,
        topic.c_str(),
        0,  // partition 0 (assumed single-partition topic)
        &low,
        &high,
        timeout_ms
    );

    rd_kafka_destroy(rk);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw diaspora::Exception{std::string("Failed to query offsets: ") + rd_kafka_err2str(err)};
    }

    return high; // high watermark = last offset + 1
}


}

#endif
