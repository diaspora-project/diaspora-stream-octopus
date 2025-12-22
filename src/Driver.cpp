#include "octopus/Driver.hpp"
#include "octopus/KafkaConf.hpp"
#include "KafkaHelper.hpp"
#include <algorithm>
#include <string>

namespace octopus {

DIASPORA_REGISTER_DRIVER(octopus, octopus, OctopusDriver);

using namespace std::string_literals;

void OctopusDriver::createTopic(std::string_view name,
                                const diaspora::Metadata& options,
                                std::shared_ptr<diaspora::ValidatorInterface> validator,
                                std::shared_ptr<diaspora::PartitionSelectorInterface> selector,
                                std::shared_ptr<diaspora::SerializerInterface> serializer) {
    if(!options.json().is_object())
        throw diaspora::Exception{"Invalid config passed to OctopusDriver::createTopic: should be an object"};

    KafkaConf kconf{m_options.json()["kafka"]};

    // Get the options
    size_t num_partitions = 1;
    if(options.json().contains("num_partitions")) {
        if(!options.json()["num_partitions"].is_number())
            throw diaspora::Exception{"\"num_partitions\" option should be a number"};
        if(options.json()["num_partitions"].get<ssize_t>() <= 0)
            throw diaspora::Exception{"Invalid value for \"num_partitions\" option"};
        num_partitions = options.json()["num_partitions"].get<size_t>();
    }

    size_t replication_factor = 3;
    if(options.json().contains("replication_factor")) {
        if(!options.json()["replication_factor"].is_number())
            throw diaspora::Exception{"\"replication_factor\" option should be a number"};
        if(options.json()["replication_factor"].get<ssize_t>() <= 0)
            throw diaspora::Exception{"Invalid value for \"replication_factor\" option"};
        replication_factor = options.json()["replication_factor"].get<size_t>();
    }

    // Create a producer instance
    char errstr[512];
    auto conf = kconf.dup(); // rd_kafka_new will take ownership if successful
    applyAwsAuthIfConfigured(conf, m_options.json()["kafka"]);
    auto rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{"Could not create rd_kafka_t instance: " + std::string{errstr}};
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    // Create the NewTopic object for <name> topic
    auto new_topic = rd_kafka_NewTopic_new(
        name.data(), num_partitions, replication_factor, errstr, sizeof(errstr));
    if (!new_topic) throw diaspora::Exception{"Failed to create NewTopic object: " + std::string{errstr}};
    auto _new_topic = std::shared_ptr<rd_kafka_NewTopic_s>{new_topic, rd_kafka_NewTopic_destroy};

    // Create the NewTopic object for __info_<name> topic
    auto info_topic_name = "__info_"s + std::string{name};
    auto new_info_topic =
        rd_kafka_NewTopic_new(info_topic_name.data(), 1, replication_factor, errstr, sizeof(errstr));
    if (!new_info_topic) throw diaspora::Exception{"Failed to create NewTopic object: " + std::string{errstr}};
    auto _new_info_topic = std::shared_ptr<rd_kafka_NewTopic_s>{new_info_topic, rd_kafka_NewTopic_destroy};

    // Create an admin options object
    auto admin_options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);
    if (!admin_options) throw diaspora::Exception{"Failed to create rd_kafka_AdminOptions_t"};
    auto _admin_options = std::shared_ptr<rd_kafka_AdminOptions_s>{admin_options, rd_kafka_AdminOptions_destroy};

    // Create a queue for the result of the operation
    auto queue = std::shared_ptr<rd_kafka_queue_t>{rd_kafka_queue_new(rk), rd_kafka_queue_destroy};

    // Initiate the topic creation
    rd_kafka_NewTopic_t *new_topics[] = {new_topic, new_info_topic};
    rd_kafka_CreateTopics(rk, new_topics, 2, admin_options, queue.get());

    // Wait for the result for up to 10 seconds
    auto event = rd_kafka_queue_poll(queue.get(), 10000);
    if (!event) throw diaspora::Exception{"Timed out waiting for CreateTopics result"};
    auto _event = std::shared_ptr<rd_kafka_event_t>{event, rd_kafka_event_destroy};

    // Check if the event type is CreateTopics result
    if (rd_kafka_event_type(event) != RD_KAFKA_EVENT_CREATETOPICS_RESULT)
        throw diaspora::Exception{"Unexpected event type when waiting for CreateTopics"};

    // Extract the result from the event
    auto result = rd_kafka_event_CreateTopics_result(event);
    size_t topic_count;
    auto topics_result = rd_kafka_CreateTopics_result_topics(result, &topic_count);
    if (topic_count != 2)
        throw diaspora::Exception{
            "Invalid number of topic results returned by rd_kafka_CreateTopics_result_topics"};

    // Check the results for errors
    for (size_t i = 0; i < topic_count; i++) {
        const rd_kafka_topic_result_t *topic_result = topics_result[i];
        if (rd_kafka_topic_result_error(topic_result) != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw diaspora::Exception{"Failed to create topic: "
                + std::string{rd_kafka_err2str(rd_kafka_topic_result_error(topic_result))}};
        }
    }

    // Produce the metadata to the info topic
    auto validator_metadata = validator->metadata().json().dump();
    rd_kafka_resp_err_t err = rd_kafka_producev(
        rk,
        RD_KAFKA_V_TOPIC(info_topic_name.c_str()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE(validator_metadata.data(), validator_metadata.size()),
        RD_KAFKA_V_END);
    if (err)
        throw diaspora::Exception{
            "Failed to produce validator metadata: " + std::string{rd_kafka_err2str(err)}};

    // Produce the selector metadata
    auto selector_metadata = selector->metadata().json().dump();
    err = rd_kafka_producev(
        rk,
        RD_KAFKA_V_TOPIC(info_topic_name.c_str()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE(selector_metadata.data(), selector_metadata.size()),
        RD_KAFKA_V_END);
    if (err)
        throw diaspora::Exception{
            "Failed to produce selector metadata: " + std::string{rd_kafka_err2str(err)}};

    // Produce the serializer metadata
    auto serializer_metadata = serializer->metadata().json().dump();
    err = rd_kafka_producev(
        rk,
        RD_KAFKA_V_TOPIC(info_topic_name.c_str()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE(serializer_metadata.data(), serializer_metadata.size()),
        RD_KAFKA_V_END);
    if (err)
        throw diaspora::Exception{
            "Failed to produce serializer metadata: " + std::string{rd_kafka_err2str(err)}};

    // Flush the producer to ensure the messages are sent
    err = rd_kafka_flush(rk, 10000);
    if (err)
        throw diaspora::Exception{
            "Failed to flush producer: " + std::string{rd_kafka_err2str(err)}};
}

std::shared_ptr<diaspora::TopicHandleInterface> OctopusDriver::openTopic(
        std::string_view name) const {
    // Create a consumer for the info topic
    auto info_topic_name = "__info_"s + std::string{name};
    // Get info from topic
    auto info_vector = readFullTopic(info_topic_name, m_options.json()["kafka"]);
    if(info_vector.size() < 3)
        throw diaspora::Exception{"Information about topic not complete in " + info_topic_name};
    auto validator = diaspora::Validator::FromMetadata(info_vector[0]);
    auto selector = diaspora::PartitionSelector::FromMetadata(info_vector[1]);
    auto serializer = diaspora::Serializer::FromMetadata(info_vector[2]);

    // Get the number of partitions
    char errstr[512];
    auto kconf = KafkaConf{m_options.json()["kafka"]};
    auto conf = kconf.dup();
    applyAwsAuthIfConfigured(conf, m_options.json()["kafka"]);
    auto rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    auto _rk = std::shared_ptr<rd_kafka_s>{rk, rd_kafka_destroy};
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{
            std::string{"Failed to create Kafka handle: "} + errstr};
    }

    /* Fetch metadata for the topic */
    const rd_kafka_metadata_t *metadata;
    rd_kafka_resp_err_t err = rd_kafka_metadata(
        rk,                      // Kafka handle
        0,                       // all_topics=0 means only this topic
        rd_kafka_topic_new(rk, name.data(), nullptr), // topic handle
        &metadata,               // output pointer
        5000                     // timeout in ms
    );

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw diaspora::Exception{
            std::string{"Failed to get metadata: "} + rd_kafka_err2str(err)};

    auto _metadata = std::shared_ptr<const rd_kafka_metadata_t>{
        metadata, rd_kafka_metadata_destroy};

    size_t num_partitions = 0;

    /* Extract partition info */
    if (metadata->topic_cnt == 0) {
        throw diaspora::Exception{std::string{"Topic \""} + std::string{name} + "\" not found"};
    } else {
        const rd_kafka_metadata_topic_t &topic = metadata->topics[0];
        if (topic.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw diaspora::Exception{
                std::string{"Topic metadata error: "} + rd_kafka_err2str(topic.err)};
        } else {
            num_partitions = topic.partition_cnt;
        }
    }

    std::vector<diaspora::PartitionInfo> targets;
    targets.reserve(num_partitions);
    for(size_t i = 0; i < num_partitions; ++i) {
        targets.emplace_back(
            nlohmann::json{{"partition_id", i}}
        );
    }

    /* Setup the partition selector */
    selector.setPartitions(targets);

    // Create the topic handle
    return std::make_shared<OctopusTopicHandle>(
        std::string{name},
        validator,
        selector,
        serializer,
        targets,
        const_cast<OctopusDriver*>(this)->shared_from_this());
}

bool OctopusDriver::topicExists(std::string_view name) const {
    auto info_topic_name = "__info_"s + std::string{name};

    char errstr[512];

    auto kconf = KafkaConf{m_options.json()["kafka"]}.dup();
    applyAwsAuthIfConfigured(kconf, m_options.json()["kafka"]);

    auto rk = rd_kafka_new(RD_KAFKA_PRODUCER, kconf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(kconf);
        throw diaspora::Exception{"Could not create rd_kafka_t instance: " + std::string{errstr}};
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    // Create an admin options object
    auto admin_options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBETOPICS);
    if (!admin_options) throw diaspora::Exception{"Failed to create rd_kafka_AdminOptions_t"};
    auto _admin_options = std::shared_ptr<rd_kafka_AdminOptions_s>{admin_options, rd_kafka_AdminOptions_destroy};

    // Create a queue for the result of the operation
    auto queue = std::shared_ptr<rd_kafka_queue_t>{
        rd_kafka_queue_new(rk),
        rd_kafka_queue_destroy};

    // Create a topic collection
    const char* topic_names[] = {name.data(), info_topic_name.data()};
    auto topic_collection = rd_kafka_TopicCollection_of_topic_names(topic_names, 2);
    if (!topic_collection) throw diaspora::Exception{"Failed to create rd_kafka_TopicCollection_t"};
    auto _topic_collection = std::shared_ptr<rd_kafka_TopicCollection_t>{
        topic_collection, rd_kafka_TopicCollection_destroy};

    // Initiate the topic description
    rd_kafka_DescribeTopics(rk, topic_collection, admin_options, queue.get());

    // Wait for the result for up to 10 seconds
    auto event = rd_kafka_queue_poll(queue.get(), 10000);
    if (!event) throw diaspora::Exception{"Timed out waiting for DescribeTopics result"};
    auto _event = std::shared_ptr<rd_kafka_event_t>{event, rd_kafka_event_destroy};

    // Check if the event type is DescribeTopics result
    if (rd_kafka_event_type(event) != RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT)
        throw diaspora::Exception{"Unexpected event type when waiting for DescribeTopics"};

    // Extract the result from the event
    auto result = rd_kafka_event_DescribeTopics_result(event);
    size_t topic_count;
    auto topics_result = rd_kafka_DescribeTopics_result_topics(result, &topic_count);
    if (topic_count != 2)
        throw diaspora::Exception{
            "Invalid number of topic results returned by rd_kafka_DescribeTopics_result_topics"};

    // Check the results for errors
    for(size_t i = 0; i < topic_count; ++i) {
        auto err = rd_kafka_TopicDescription_error(topics_result[i]);
        if (!err) continue;
        if (rd_kafka_error_code(err) == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART)
            return false;
        throw diaspora::Exception{"Failed to describe topic: "
            + std::string{rd_kafka_error_string(err)}};
    }
    return true;
}

std::unordered_map<std::string, diaspora::Metadata> OctopusDriver::listTopics() const {
    char errstr[512];

    auto kconf = KafkaConf{m_options.json()["kafka"]};
    auto conf = kconf.dup();
    applyAwsAuthIfConfigured(conf, m_options.json()["kafka"]);
    auto rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{
            std::string{"Failed to create Kafka handle: "} + errstr};
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    // Fetch metadata for all topics
    const rd_kafka_metadata_t *metadata;
    rd_kafka_resp_err_t err = rd_kafka_metadata(
        rk,          // Kafka handle
        1,           // all_topics=1 means all topics
        nullptr,     // no specific topic
        &metadata,   // output pointer
        5000         // timeout in ms
    );

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw diaspora::Exception{
            std::string{"Failed to get metadata: "} + rd_kafka_err2str(err)};

    auto _metadata = std::shared_ptr<const rd_kafka_metadata_t>{
        metadata, rd_kafka_metadata_destroy};

    std::unordered_map<std::string, diaspora::Metadata> result;

    // Iterate through all topics
    for (int i = 0; i < metadata->topic_cnt; i++) {
        const rd_kafka_metadata_topic_t &topic = metadata->topics[i];
        std::string topic_name = topic.topic;

        // Skip __info_ topics (these are internal metadata topics)
        if (topic_name.rfind("__info_", 0) == 0) {
            continue;
        }

        // Try to read the info topic for this topic
        auto info_topic_name = "__info_"s + topic_name;

        try {
            auto info_vector = readFullTopic(info_topic_name, m_options.json()["kafka"]);
            if (info_vector.size() >= 3) {
                // We have valid metadata - construct the metadata JSON
                nlohmann::json metadata_json;
                metadata_json["validator"] = nlohmann::json::parse(info_vector[0]);
                metadata_json["selector"] = nlohmann::json::parse(info_vector[1]);
                metadata_json["serializer"] = nlohmann::json::parse(info_vector[2]);
                metadata_json["num_partitions"] = topic.partition_cnt;

                result[topic_name] = diaspora::Metadata{metadata_json};
            }
        } catch (...) {
            // If we can't read the info topic, skip this topic
            // (it might not be a Diaspora topic or the info topic doesn't exist)
            continue;
        }
    }

    return result;
}

std::shared_ptr<diaspora::ThreadPoolInterface> OctopusDriver::defaultThreadPool() const {
    return m_default_thread_pool;
}

std::shared_ptr<diaspora::ThreadPoolInterface> OctopusDriver::makeThreadPool(diaspora::ThreadCount count) const {
    return std::make_shared<diaspora::PosixThreadPool>(count);
}

std::shared_ptr<diaspora::DriverInterface> OctopusDriver::create(const diaspora::Metadata& options) {
    auto& config = options.json();
    if(!config.is_object())
        throw diaspora::Exception{
            "OctopusDriver configuration file doesn't have a correct format"};
    if(!config.contains("kafka") || !config["kafka"].is_object())
        throw diaspora::Exception{
            "OctopusDriver configuration file doesn't have a correct format"
            " (expected a \"kafka\" object field)"};
    auto& kafka_options = config["kafka"];
    if(!kafka_options.contains("bootstrap.servers"))
        throw diaspora::Exception{
            "\"bootstrap.servers\" not found or not a string in OctopusDriver configuration"};
    if(!kafka_options["bootstrap.servers"].is_string()) {
        if(!kafka_options["bootstrap.servers"].is_array())
            throw diaspora::Exception{"\"bootstrap.servers\" should be a string or an array of strings"};
        bool all_strings = std::accumulate(kafka_options["bootstrap.servers"].begin(),
                                           kafka_options["bootstrap.servers"].end(),
                                           true,
                                           [](bool acc, auto& e) { return acc && e.is_string(); });
        if(!all_strings)
            throw diaspora::Exception{"\"bootstrap.servers\" should be a string or an array of strings"};
    }
    return std::make_shared<OctopusDriver>(options);
}

}
