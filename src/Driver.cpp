#include "octopus/Driver.hpp"
#include "octopus/KafkaConf.hpp"
#include <uuid.h>
#include <algorithm>
#include <string>

namespace octopus {

DIASPORA_REGISTER_DRIVER(octopus, octopus, OctopusDriver);

using namespace std::string_literals;

static std::shared_ptr<rd_kafka_t> createInfoConsumer(const std::string& topic_name,
                                                      KafkaConf conf) {
    char errstr[512];

    uuid_t consumer_uuid;
    uuid_generate(consumer_uuid);
    char group_id[37] = {0};
    uuid_unparse(consumer_uuid, group_id);
    conf["group.id"] = std::string{"info-consurmer-"} + group_id;
    conf["auto.offset.reset"] = "earliest";
    conf["topic.metadata.refresh.interval.ms"] = "10000";

    auto kconf = conf.dup();

    auto info_consumer = std::shared_ptr<rd_kafka_t>{
        rd_kafka_new(RD_KAFKA_CONSUMER, kconf, errstr, sizeof(errstr)),
        rd_kafka_destroy};
    if (!info_consumer) {
        rd_kafka_conf_destroy(kconf);
        throw diaspora::Exception{
            "Could not create rd_kafka_t instance: " + std::string{errstr}};
    }

    // Create the topic object
    auto info_topic = std::shared_ptr<rd_kafka_topic_t>{
        rd_kafka_topic_new(info_consumer.get(), topic_name.data(), nullptr),
        rd_kafka_topic_destroy};
    if (!info_topic)
        throw diaspora::Exception{
            "Failed to create topic object: " + std::string{rd_kafka_err2str(rd_kafka_last_error())}};

    // Create a topic partition list
    auto topic_partition_list = std::shared_ptr<rd_kafka_topic_partition_list_t>{
        rd_kafka_topic_partition_list_new(1),
        rd_kafka_topic_partition_list_destroy};
    if (!topic_partition_list)
        throw diaspora::Exception{
            "Failed to create topic partition list: " + std::string{rd_kafka_err2str(rd_kafka_last_error())}};

    // Add the topic to the list
    rd_kafka_topic_partition_list_add(topic_partition_list.get(), topic_name.data(), RD_KAFKA_PARTITION_UA);

    // Subscribe to the topic
    rd_kafka_resp_err_t err = rd_kafka_subscribe(info_consumer.get(), topic_partition_list.get());
    if (err)
        throw diaspora::Exception{
            "Failed to subscribe to topic: " + std::string{rd_kafka_err2str(err)}};

    return info_consumer;
}

static std::string consumeMessage(std::shared_ptr<rd_kafka_t> info_consumer) {
    rd_kafka_message_t *message;
    while (true) {
        message = rd_kafka_consumer_poll(info_consumer.get(), 1000);
        if (message) {
            if (message->err) {
                if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    // End of partition, continue polling
                    rd_kafka_message_destroy(message);
                    continue;
                } else {
                    throw diaspora::Exception{
                        "Consumer error: " + std::string{rd_kafka_message_errstr(message)}};
                }
            } else {
                // Message received
                std::string payload(static_cast<char*>(message->payload), message->len);
                rd_kafka_message_destroy(message);
                return payload;
            }
        } else {
            // Timeout, continue polling
            continue;
        }
    }
}

static void consumeMetadata(std::string_view topic_name,
                            std::shared_ptr<rd_kafka_t> info_consumer,
                            diaspora::Validator& validator,
                            diaspora::PartitionSelector& selector,
                            diaspora::Serializer& serializer) {
    auto info_topic_name = "__info_"s + std::string{topic_name};
    // Consume the validator metadata
    auto validator_metadata = consumeMessage(info_consumer);
    validator = diaspora::Validator::FromMetadata(
        diaspora::Metadata{validator_metadata}
    );

    // Consume the selector metadata
    auto selector_metadata = consumeMessage(info_consumer);
    selector = diaspora::PartitionSelector::FromMetadata(
        diaspora::Metadata{selector_metadata}
    );

    // Consume the serializer metadata
    auto serializer_metadata = consumeMessage(info_consumer);
    serializer = diaspora::Serializer::FromMetadata(
        diaspora::Metadata{serializer_metadata}
    );
}

void OctopusDriver::createTopic(std::string_view name,
                                const diaspora::Metadata& options,
                                std::shared_ptr<diaspora::ValidatorInterface> validator,
                                std::shared_ptr<diaspora::PartitionSelectorInterface> selector,
                                std::shared_ptr<diaspora::SerializerInterface> serializer) {
    if(!options.json().is_object())
        throw diaspora::Exception{"Invalid config passed to OctopusDriver::createTopic: should be an object"};

    KafkaConf kconf{m_options};

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
    auto rk = std::shared_ptr<rd_kafka_t>{
        rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)),
        rd_kafka_destroy};
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{"Could not create rd_kafka_t instance: " + std::string{errstr}};
    }

    // Create the NewTopic object for <name> topic
    auto new_topic = std::shared_ptr<rd_kafka_NewTopic_s>{
        rd_kafka_NewTopic_new(name.data(), num_partitions, replication_factor, errstr, sizeof(errstr)),
        rd_kafka_NewTopic_destroy};
    if (!new_topic) throw diaspora::Exception{"Failed to create NewTopic object: " + std::string{errstr}};

    // Create the NewTopic object for __info_<name> topic
    auto info_topic_name = "__info_"s + std::string{name};
    auto new_info_topic = std::shared_ptr<rd_kafka_NewTopic_s>{
        rd_kafka_NewTopic_new(info_topic_name.data(), 1, replication_factor, errstr, sizeof(errstr)),
        rd_kafka_NewTopic_destroy};
    if (!new_info_topic) throw diaspora::Exception{"Failed to create NewTopic object: " + std::string{errstr}};

    // Create an admin options object
    auto admin_options = std::shared_ptr<rd_kafka_AdminOptions_s>{
        rd_kafka_AdminOptions_new(rk.get(), RD_KAFKA_ADMIN_OP_CREATETOPICS),
        rd_kafka_AdminOptions_destroy};
    if (!admin_options) throw diaspora::Exception{"Failed to create rd_kafka_AdminOptions_t"};

    // Create a queue for the result of the operation
    auto queue = std::shared_ptr<rd_kafka_queue_t>{
        rd_kafka_queue_new(rk.get()),
        rd_kafka_queue_destroy};

    // Initiate the topic creation
    rd_kafka_NewTopic_t *new_topics[] = {new_topic.get(), new_info_topic.get()};
    rd_kafka_CreateTopics(rk.get(), new_topics, 2, admin_options.get(), queue.get());

    // Wait for the result for up to 10 seconds
    auto event = std::shared_ptr<rd_kafka_event_t>{
        rd_kafka_queue_poll(queue.get(), 10000),
        rd_kafka_event_destroy};
    if (!event) throw diaspora::Exception{"Timed out waiting for CreateTopics result"};

    // Check if the event type is CreateTopics result
    if (rd_kafka_event_type(event.get()) != RD_KAFKA_EVENT_CREATETOPICS_RESULT)
        throw diaspora::Exception{"Unexpected event type when waiting for CreateTopics"};

    // Extract the result from the event
    auto result = rd_kafka_event_CreateTopics_result(event.get());
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
        rk.get(),
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
        rk.get(),
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
        rk.get(),
        RD_KAFKA_V_TOPIC(info_topic_name.c_str()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE(serializer_metadata.data(), serializer_metadata.size()),
        RD_KAFKA_V_END);
    if (err)
        throw diaspora::Exception{
            "Failed to produce serializer metadata: " + std::string{rd_kafka_err2str(err)}};

    // Flush the producer to ensure the messages are sent
    err = rd_kafka_flush(rk.get(), 10000);
    if (err)
        throw diaspora::Exception{
            "Failed to flush producer: " + std::string{rd_kafka_err2str(err)}};
}

std::shared_ptr<diaspora::TopicHandleInterface> OctopusDriver::openTopic(
        std::string_view name) const {
    // Create a consumer for the info topic
    auto info_topic_name = "__info_"s + std::string{name};
    auto info_consumer = createInfoConsumer(info_topic_name, m_options);

    // Consume the metadata from the info topic
    diaspora::Validator validator;
    diaspora::PartitionSelector selector;
    diaspora::Serializer serializer;
    consumeMetadata(name, info_consumer, validator, selector, serializer);

    // Get the number of partitions
    char errstr[512];
    auto kconf = KafkaConf{m_options};
    auto conf = kconf.dup();
    auto rk = std::shared_ptr<rd_kafka_s>{
        rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)),
        rd_kafka_destroy};
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{
            std::string{"Failed to create Kafka handle: "} + errstr};
    }

    /* Fetch metadata for the topic */
    const rd_kafka_metadata_t *metadata;
    rd_kafka_resp_err_t err = rd_kafka_metadata(
        rk.get(),                // Kafka handle
        0,                       // all_topics=0 means only this topic
        rd_kafka_topic_new(rk.get(), name.data(), nullptr), // topic handle
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

    auto kconf = KafkaConf{m_options}.dup();

    auto rk = std::shared_ptr<rd_kafka_t>{
        rd_kafka_new(RD_KAFKA_PRODUCER, kconf, errstr, sizeof(errstr)),
        rd_kafka_destroy};
    if (!rk) {
        rd_kafka_conf_destroy(kconf);
        throw diaspora::Exception{"Could not create rd_kafka_t instance: " + std::string{errstr}};
    }

    // Create an admin options object
    auto admin_options = std::shared_ptr<rd_kafka_AdminOptions_s>{
        rd_kafka_AdminOptions_new(rk.get(), RD_KAFKA_ADMIN_OP_DESCRIBETOPICS),
        rd_kafka_AdminOptions_destroy};
    if (!admin_options) throw diaspora::Exception{"Failed to create rd_kafka_AdminOptions_t"};

    // Create a queue for the result of the operation
    auto queue = std::shared_ptr<rd_kafka_queue_t>{
        rd_kafka_queue_new(rk.get()),
        rd_kafka_queue_destroy};

    // Create a topic collection
    const char* topic_names[] = {name.data(), info_topic_name.data()};
    auto topic_collection = std::shared_ptr<rd_kafka_TopicCollection_t>{
        rd_kafka_TopicCollection_of_topic_names(topic_names, 2),
        rd_kafka_TopicCollection_destroy};
    if (!topic_collection) throw diaspora::Exception{"Failed to create rd_kafka_TopicCollection_t"};

    // Initiate the topic description
    rd_kafka_DescribeTopics(rk.get(), topic_collection.get(), admin_options.get(), queue.get());

    // Wait for the result for up to 10 seconds
    auto event = std::shared_ptr<rd_kafka_event_t>{
        rd_kafka_queue_poll(queue.get(), 10000),
        rd_kafka_event_destroy};
    if (!event) throw diaspora::Exception{"Timed out waiting for DescribeTopics result"};

    // Check if the event type is DescribeTopics result
    if (rd_kafka_event_type(event.get()) != RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT)
        throw diaspora::Exception{"Unexpected event type when waiting for DescribeTopics"};

    // Extract the result from the event
    auto result = rd_kafka_event_DescribeTopics_result(event.get());
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

std::shared_ptr<diaspora::ThreadPoolInterface> OctopusDriver::defaultThreadPool() const {
    return m_default_thread_pool;
}

std::shared_ptr<diaspora::ThreadPoolInterface> OctopusDriver::makeThreadPool(diaspora::ThreadCount count) const {
    return std::make_shared<OctopusThreadPool>(count);
}

std::shared_ptr<diaspora::DriverInterface> OctopusDriver::create(const diaspora::Metadata& options) {
    auto& config = options.json();
    if(!config.is_object())
        throw diaspora::Exception{"OctopusDriver configuration file doesn't have a correct format"};
    if(!config.contains("bootstrap.servers"))
        throw diaspora::Exception{
            "\"bootstrap.servers\" not found or not a string in OctopusDriver configuration"};
    if(!config["bootstrap.servers"].is_string()) {
        if(!config["bootstrap.servers"].is_array())
            throw diaspora::Exception{"\"bootstrap.servers\" should be a string or an array of strings"};
        bool all_strings = std::accumulate(config["bootstrap.servers"].begin(),
                                           config["bootstrap.servers"].end(),
                                           true,
                                           [](bool acc, auto& e) { return acc && e.is_string(); });
        if(!all_strings)
            throw diaspora::Exception{"\"bootstrap.servers\" should be a string or an array of strings"};
    }
    return std::make_shared<OctopusDriver>(options);
}

}
