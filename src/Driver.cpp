#include "octopus/Driver.hpp"
#include "octopus/KafkaConf.hpp"
#include <algorithm>

namespace octopus {

DIASPORA_REGISTER_DRIVER(octopus, octopus, OctopusDriver);

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
    }

    size_t replication_factor = 3;
    if(options.json().contains("replication_factor")) {
        if(!options.json()["replication_factor"].is_number())
            throw diaspora::Exception{"\"replication_factor\" option should be a number"};
        if(options.json()["replication_factor"].get<ssize_t>() <= 0)
            throw diaspora::Exception{"Invalid value for \"replication_factor\" option"};
    }

    char errstr[512];
    auto rk = std::shared_ptr<rd_kafka_t>{
        rd_kafka_new(RD_KAFKA_PRODUCER, kconf, errstr, sizeof(errstr)),
        rd_kafka_destroy};
    if (!rk) throw diaspora::Exception{"Could not create rd_kafka_t instance: " + std::string{errstr}};

    // Create the NewTopic object
    auto new_topic = std::shared_ptr<rd_kafka_NewTopic_s>{
        rd_kafka_NewTopic_new(name.data(), num_partitions, replication_factor, errstr, sizeof(errstr)),
        rd_kafka_NewTopic_destroy};
    if (!new_topic) throw diaspora::Exception{"Failed to create NewTopic object: " + std::string{errstr}};

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
    rd_kafka_NewTopic_t *new_topics[] = {new_topic.get()};
    rd_kafka_CreateTopics(rk.get(), new_topics, 1, admin_options.get(), queue.get());

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

    // Check the results for errors
    for (size_t i = 0; i < topic_count; i++) {
        const rd_kafka_topic_result_t *topic_result = topics_result[i];
        if (rd_kafka_topic_result_error(topic_result) != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw diaspora::Exception{"Failed to create topic: "
                + std::string{rd_kafka_err2str(rd_kafka_topic_result_error(topic_result))}};
        }
    }
}

std::shared_ptr<diaspora::TopicHandleInterface> OctopusDriver::openTopic(
        std::string_view name) const {
    // TODO
}

bool OctopusDriver::topicExists(std::string_view name) const {
    // TODO
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
