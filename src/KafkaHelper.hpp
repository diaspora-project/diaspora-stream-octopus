#ifndef OCTOPUS_KAFKA_HELPER_H
#define OCTOPUS_KAFKA_HELPER_H

#include "octopus/KafkaConf.hpp"
#include <diaspora/Exception.hpp>
#include <uuid.h>
#include <librdkafka/rdkafka.h>
#include <string>
#include <memory>

#ifdef OCTOPUS_HAS_AWS
#include "AwsMskIamSigner.hpp"
#endif

namespace octopus {

#ifdef OCTOPUS_HAS_AWS
/**
 * @brief Sets up AWS MSK IAM authentication for a Kafka configuration.
 *
 * This function configures the rd_kafka_conf_t with the necessary settings
 * for AWS MSK IAM authentication and sets up the OAuth token refresh callback.
 *
 * @param conf The Kafka configuration object to modify
 * @param region The AWS region for the MSK cluster
 */
static inline void setupAwsMskIamAuth(rd_kafka_conf_t* conf, const std::string& region) {
    char errstr[512];

    // Set security protocol and SASL mechanism
    if (rd_kafka_conf_set(conf, "security.protocol", "SASL_SSL",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        throw diaspora::Exception{"Failed to set security.protocol: " + std::string{errstr}};
    }

    if (rd_kafka_conf_set(conf, "sasl.mechanisms", "OAUTHBEARER",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        throw diaspora::Exception{"Failed to set sasl.mechanisms: " + std::string{errstr}};
    }

    // Set up the OAuth bearer token refresh callback
    // We capture the region by value in a heap-allocated string that will live
    // for the lifetime of the configuration
    auto region_ptr = new std::string(region);

    rd_kafka_conf_set_oauthbearer_token_refresh_cb(
        conf,
        [](rd_kafka_t* rk, const char* oauthbearer_config, void* opaque) {
            (void)oauthbearer_config; // Unused parameter
            try {
                std::string* region = static_cast<std::string*>(opaque);
                AwsMskIamSigner signer(*region);
                auto token = signer.generateToken();
                char errstr_token[512];
                rd_kafka_resp_err_t err = rd_kafka_oauthbearer_set_token(
                    rk,
                    token.token.c_str(),
                    token.expiration_ms,
                    "",
                    nullptr, 0,
                    errstr_token,
                    sizeof(errstr_token));
                if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    rd_kafka_oauthbearer_set_token_failure(rk, errstr_token);
                }
            } catch (const std::exception& ex) {
                rd_kafka_oauthbearer_set_token_failure(rk, ex.what());
            }
        }
    );

    // Set the opaque pointer to our region string
    rd_kafka_conf_set_opaque(conf, region_ptr);
}
#endif

/**
 * @brief Configures AWS MSK IAM authentication if enabled in the configuration.
 *
 * This function checks if the Kafka configuration contains AWS MSK IAM settings
 * and applies them if present. The configuration should contain an "aws_msk_iam"
 * object with a "region" field.
 *
 * Example configuration:
 * {
 *   "kafka": {
 *     "bootstrap.servers": "...",
 *     "aws_msk_iam": {
 *       "region": "us-east-1"
 *     }
 *   }
 * }
 *
 * @param conf The rd_kafka_conf_t to configure
 * @param kafka_config The JSON configuration object containing Kafka settings
 */
static inline void applyAwsAuthIfConfigured(rd_kafka_conf_t* conf, const nlohmann::json& kafka_config) {
#ifdef OCTOPUS_HAS_AWS
    if (kafka_config.is_object() &&
        kafka_config.contains("aws_msk_iam") &&
        kafka_config["aws_msk_iam"].is_object()) {

        const auto& aws_config = kafka_config["aws_msk_iam"];

        if (aws_config.contains("region") && aws_config["region"].is_string()) {
            std::string region = aws_config["region"].get<std::string>();
            setupAwsMskIamAuth(conf, region);
        } else {
            throw diaspora::Exception{
                "AWS MSK IAM configuration requires a 'region' field"};
        }
    }
#else
    (void)conf;
    (void)kafka_config;
#endif
}

/**
 * This function reads a full topic (used to read an info topic). The topic should
 * only have one partition, since consumption will stop at the first PARTITION_EOF
 * received.
 */
static inline std::vector<std::string> readFullTopic(std::string_view name, const nlohmann::json& kafka_config) {

    char errstr[512];

    KafkaConf conf{kafka_config};
    uuid_t consumer_uuid;
    uuid_generate(consumer_uuid);
    char group_id[37] =  {0};
    uuid_unparse(consumer_uuid, group_id);
    conf["group.id"] = std::string{"info-consurmer-"} + group_id;
    conf["auto.offset.reset"] = "earliest";
    conf["topic.metadata.refresh.interval.ms"] = "10000";
    conf["enable.partition.eof"] = "true";

    auto kconf = conf.dup();
    applyAwsAuthIfConfigured(kconf, kafka_config);

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
static inline int64_t getNumEventsInPartition(const nlohmann::json& kafka_config,
                                              const std::string& topic,
                                              int timeout_ms = 5000)
{
    char errstr[512];

    // Create a temporary Kafka handle (producer works fine)
    KafkaConf kconf{kafka_config};
    rd_kafka_conf_t *conf = kconf.dup();
    applyAwsAuthIfConfigured(conf, kafka_config);
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
