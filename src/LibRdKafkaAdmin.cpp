#include "LibRdKafkaAdmin.hpp"
#include "octopus/KafkaConf.hpp"
#include "KafkaHelper.hpp"
#include <diaspora/Exception.hpp>
#include <librdkafka/rdkafka.h>
#include <uuid.h>
#include <future>
#include <memory>

namespace octopus {

using namespace std::string_literals;

/**
 * @brief Result of an admin operation, extracted from the rd_kafka_event_t
 * inside the background event callback.
 */
struct AdminResult {
    rd_kafka_event_type_t event_type = RD_KAFKA_EVENT_NONE;
    struct TopicResult {
        rd_kafka_resp_err_t err;
        std::string error_string;
    };
    std::vector<TopicResult> topic_results;
};

/**
 * @brief Background event callback registered via rd_kafka_conf_set_background_event_cb.
 * Extracts admin operation results from the event and fulfills the promise
 * passed via rd_kafka_AdminOptions_set_opaque.
 */
static void adminBackgroundCb(rd_kafka_t* /*rk*/, rd_kafka_event_t* rkev, void* /*opaque*/) {
    auto* promise = static_cast<std::promise<AdminResult>*>(rd_kafka_event_opaque(rkev));
    if (!promise) return;

    AdminResult result;
    result.event_type = rd_kafka_event_type(rkev);

    switch (result.event_type) {
        case RD_KAFKA_EVENT_CREATETOPICS_RESULT: {
            size_t cnt;
            auto topics = rd_kafka_CreateTopics_result_topics(
                rd_kafka_event_CreateTopics_result(rkev), &cnt);
            for (size_t i = 0; i < cnt; i++) {
                result.topic_results.push_back({
                    rd_kafka_topic_result_error(topics[i]),
                    rd_kafka_err2str(rd_kafka_topic_result_error(topics[i]))});
            }
            break;
        }
        case RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT: {
            size_t cnt;
            auto topics = rd_kafka_DescribeTopics_result_topics(
                rd_kafka_event_DescribeTopics_result(rkev), &cnt);
            for (size_t i = 0; i < cnt; i++) {
                auto err = rd_kafka_TopicDescription_error(topics[i]);
                if (err) {
                    result.topic_results.push_back({
                        rd_kafka_error_code(err),
                        rd_kafka_error_string(err) ? rd_kafka_error_string(err) : ""});
                } else {
                    result.topic_results.push_back({RD_KAFKA_RESP_ERR_NO_ERROR, ""});
                }
            }
            break;
        }
        default:
            break;
    }

    promise->set_value(std::move(result));
}

LibRdKafkaAdmin::LibRdKafkaAdmin(const nlohmann::json& config, std::string ns)
: m_config(config)
, m_namespace(std::move(ns)) {}

std::string LibRdKafkaAdmin::kafkaTopicName(std::string_view name) const {
    if(m_namespace.empty()) return std::string{name};
    return m_namespace + "." + std::string{name};
}

void LibRdKafkaAdmin::createTopics(const std::vector<TopicSpec>& topics) const {
    char errstr[512];

    KafkaConf kconf{m_config["kafka"]};
    auto conf = kconf.dup();
    applyAwsAuthIfConfigured(conf, m_config);
    rd_kafka_conf_set_background_event_cb(conf, adminBackgroundCb);

    auto rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{"Could not create rd_kafka_t instance: " + std::string{errstr}};
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    // Create NewTopic objects
    std::vector<rd_kafka_NewTopic_t*> new_topics;
    std::vector<std::shared_ptr<rd_kafka_NewTopic_s>> new_topic_guards;
    new_topics.reserve(topics.size());
    new_topic_guards.reserve(topics.size());

    for (auto& spec : topics) {
        auto kafka_name = kafkaTopicName(spec.name);
        auto new_topic = rd_kafka_NewTopic_new(
            kafka_name.c_str(), spec.num_partitions, spec.replication_factor,
            errstr, sizeof(errstr));
        if (!new_topic)
            throw diaspora::Exception{"Failed to create NewTopic object: " + std::string{errstr}};
        new_topic_guards.emplace_back(new_topic, rd_kafka_NewTopic_destroy);
        new_topics.push_back(new_topic);
    }

    // Set up promise/future for the background callback
    std::promise<AdminResult> promise;
    auto future = promise.get_future();

    auto admin_options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);
    if (!admin_options) throw diaspora::Exception{"Failed to create rd_kafka_AdminOptions_t"};
    auto _admin_options = std::shared_ptr<rd_kafka_AdminOptions_s>{admin_options, rd_kafka_AdminOptions_destroy};
    rd_kafka_AdminOptions_set_opaque(admin_options, &promise);

    auto rkqu = rd_kafka_queue_get_background(rk);
    rd_kafka_CreateTopics(rk, new_topics.data(), new_topics.size(), admin_options, rkqu);
    rd_kafka_queue_destroy(rkqu);

    // Wait for the background callback to deliver the result
    if (future.wait_for(std::chrono::seconds(60)) == std::future_status::timeout)
        throw diaspora::Exception{"Timed out waiting for CreateTopics result"};
    auto admin_result = future.get();

    if (admin_result.event_type != RD_KAFKA_EVENT_CREATETOPICS_RESULT)
        throw diaspora::Exception{"Unexpected event type when waiting for CreateTopics"};

    if (admin_result.topic_results.size() != topics.size())
        throw diaspora::Exception{
            "Invalid number of topic results returned by rd_kafka_CreateTopics_result_topics"};

    for (auto& tr : admin_result.topic_results) {
        if (tr.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw diaspora::Exception{"Failed to create topic: " + tr.error_string};
        }
    }
}

bool LibRdKafkaAdmin::topicExists(std::string_view name) const {
    auto kafka_name = kafkaTopicName(name);
    char errstr[512];

    KafkaConf kconf{m_config["kafka"]};
    auto conf = kconf.dup();
    applyAwsAuthIfConfigured(conf, m_config);
    rd_kafka_conf_set_background_event_cb(conf, adminBackgroundCb);

    auto rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{"Could not create rd_kafka_t instance: " + std::string{errstr}};
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    std::promise<AdminResult> promise;
    auto future = promise.get_future();

    auto admin_options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBETOPICS);
    if (!admin_options) throw diaspora::Exception{"Failed to create rd_kafka_AdminOptions_t"};
    auto _admin_options = std::shared_ptr<rd_kafka_AdminOptions_s>{admin_options, rd_kafka_AdminOptions_destroy};
    rd_kafka_AdminOptions_set_opaque(admin_options, &promise);

    const char* topic_names[] = {kafka_name.c_str()};
    auto topic_collection = rd_kafka_TopicCollection_of_topic_names(topic_names, 1);
    if (!topic_collection) throw diaspora::Exception{"Failed to create rd_kafka_TopicCollection_t"};
    auto _topic_collection = std::shared_ptr<rd_kafka_TopicCollection_t>{
        topic_collection, rd_kafka_TopicCollection_destroy};

    auto rkqu = rd_kafka_queue_get_background(rk);
    rd_kafka_DescribeTopics(rk, topic_collection, admin_options, rkqu);
    rd_kafka_queue_destroy(rkqu);

    if (future.wait_for(std::chrono::seconds(60)) == std::future_status::timeout)
        throw diaspora::Exception{"Timed out waiting for DescribeTopics result"};
    auto admin_result = future.get();

    if (admin_result.event_type != RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT)
        throw diaspora::Exception{"Unexpected event type when waiting for DescribeTopics"};

    if (admin_result.topic_results.size() != 1)
        throw diaspora::Exception{
            "Invalid number of topic results returned by rd_kafka_DescribeTopics_result_topics"};

    auto& tr = admin_result.topic_results[0];
    if (tr.err == RD_KAFKA_RESP_ERR_NO_ERROR) return true;
    if (tr.err == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART) return false;
    throw diaspora::Exception{"Failed to describe topic: " + tr.error_string};
}

size_t LibRdKafkaAdmin::getPartitionCount(std::string_view name) const {
    auto kafka_name = kafkaTopicName(name);
    char errstr[512];

    KafkaConf kconf{m_config["kafka"]};
    auto conf = kconf.dup();
    applyAwsAuthIfConfigured(conf, m_config);

    auto rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{
            std::string{"Failed to create Kafka handle: "} + errstr};
    }
    auto _rk = std::shared_ptr<rd_kafka_s>{rk, rd_kafka_destroy};

    const rd_kafka_metadata_t *metadata;
    rd_kafka_resp_err_t err = rd_kafka_metadata(
        rk,
        0,
        rd_kafka_topic_new(rk, kafka_name.c_str(), nullptr),
        &metadata,
        60000
    );

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw diaspora::Exception{
            std::string{"Failed to get metadata: "} + rd_kafka_err2str(err)};

    auto _metadata = std::shared_ptr<const rd_kafka_metadata_t>{
        metadata, rd_kafka_metadata_destroy};

    if (metadata->topic_cnt == 0) {
        throw diaspora::Exception{
            std::string{"Topic \""} + std::string{name} + "\" not found"};
    }

    const rd_kafka_metadata_topic_t &topic = metadata->topics[0];
    if (topic.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw diaspora::Exception{
            std::string{"Topic metadata error: "} + rd_kafka_err2str(topic.err)};
    }

    return topic.partition_cnt;
}

std::vector<TopicInfo> LibRdKafkaAdmin::listAllTopics() const {
    char errstr[512];

    KafkaConf kconf{m_config["kafka"]};
    auto conf = kconf.dup();
    applyAwsAuthIfConfigured(conf, m_config);

    auto rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{
            std::string{"Failed to create Kafka handle: "} + errstr};
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    const rd_kafka_metadata_t *metadata;
    rd_kafka_resp_err_t err = rd_kafka_metadata(
        rk, 1, nullptr, &metadata, 5000);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw diaspora::Exception{
            std::string{"Failed to get metadata: "} + rd_kafka_err2str(err)};

    auto _metadata = std::shared_ptr<const rd_kafka_metadata_t>{
        metadata, rd_kafka_metadata_destroy};

    std::vector<TopicInfo> result;

    auto ns_prefix = m_namespace.empty() ? ""s : m_namespace + ".";
    auto info_prefix = ns_prefix + "__info_";

    for (int i = 0; i < metadata->topic_cnt; i++) {
        const rd_kafka_metadata_topic_t &topic = metadata->topics[i];
        std::string kafka_topic_name = topic.topic;

        // Skip topics that don't match our namespace prefix
        if (!ns_prefix.empty() && kafka_topic_name.rfind(ns_prefix, 0) != 0) {
            continue;
        }

        // Skip __info_ topics
        if (kafka_topic_name.rfind(info_prefix, 0) == 0) {
            continue;
        }

        // Strip the namespace prefix to get the user-facing topic name
        auto user_topic_name = ns_prefix.empty()
            ? kafka_topic_name
            : kafka_topic_name.substr(ns_prefix.size());

        result.push_back({std::move(user_topic_name), static_cast<size_t>(topic.partition_cnt)});
    }

    return result;
}

void LibRdKafkaAdmin::produceMessages(std::string_view topic,
                                      const std::vector<std::string>& messages) const {
    auto kafka_name = kafkaTopicName(topic);
    char errstr[512];

    KafkaConf kconf{m_config["kafka"]};
    auto conf = kconf.dup();
    applyAwsAuthIfConfigured(conf, m_config);

    auto rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{"Could not create rd_kafka_t instance: " + std::string{errstr}};
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    for (auto& msg : messages) {
        rd_kafka_resp_err_t err = rd_kafka_producev(
            rk,
            RD_KAFKA_V_TOPIC(kafka_name.c_str()),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE(const_cast<char*>(msg.data()), msg.size()),
            RD_KAFKA_V_END);
        if (err)
            throw diaspora::Exception{
                "Failed to produce message: " + std::string{rd_kafka_err2str(err)}};
    }

    rd_kafka_resp_err_t err = rd_kafka_flush(rk, 10000);
    if (err)
        throw diaspora::Exception{
            "Failed to flush producer: " + std::string{rd_kafka_err2str(err)}};
}

std::vector<std::string> LibRdKafkaAdmin::readFullTopic(std::string_view name) const {
    auto kafka_name = kafkaTopicName(name);
    char errstr[512];

    KafkaConf conf{m_config["kafka"]};
    uuid_t consumer_uuid;
    uuid_generate(consumer_uuid);
    char group_id[37] = {0};
    uuid_unparse(consumer_uuid, group_id);
    conf["group.id"] = std::string{"info-consurmer-"} + group_id;
    conf["auto.offset.reset"] = "earliest";
    conf["topic.metadata.refresh.interval.ms"] = "10000";
    conf["enable.partition.eof"] = "true";

    auto kconf = conf.dup();
    applyAwsAuthIfConfigured(kconf, m_config);

    auto consumer = rd_kafka_new(RD_KAFKA_CONSUMER, kconf, errstr, sizeof(errstr));
    if (!consumer) {
        rd_kafka_conf_destroy(kconf);
        throw diaspora::Exception{
            "Could not create rd_kafka_t instance: " + std::string{errstr}};
    }
    auto _consumer = std::shared_ptr<rd_kafka_t>{consumer, rd_kafka_destroy};

    auto info_topic = rd_kafka_topic_new(consumer, kafka_name.data(), nullptr);
    if (!info_topic)
        throw diaspora::Exception{
            "Failed to create topic object: " + std::string{rd_kafka_err2str(rd_kafka_last_error())}};
    auto _info_topic = std::shared_ptr<rd_kafka_topic_t>{info_topic, rd_kafka_topic_destroy};

    auto topic_partition_list = rd_kafka_topic_partition_list_new(1);
    if (!topic_partition_list)
        throw diaspora::Exception{
            "Failed to create topic partition list: " + std::string{rd_kafka_err2str(rd_kafka_last_error())}};
    auto _topic_partition_list = std::shared_ptr<rd_kafka_topic_partition_list_t>{
        topic_partition_list,
        rd_kafka_topic_partition_list_destroy};

    rd_kafka_topic_partition_list_add(
        topic_partition_list, kafka_name.data(), RD_KAFKA_PARTITION_UA);

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
                result.emplace_back(static_cast<char*>(message->payload), message->len);
            }
        } else {
            continue;
        }
    }

    rd_kafka_unsubscribe(consumer);

    return result;
}

}
