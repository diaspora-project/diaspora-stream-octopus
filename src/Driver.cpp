#include "octopus/Driver.hpp"
#include "LibRdKafkaAdmin.hpp"
#include "OctopusAdmin.hpp"
#include <algorithm>
#include <iostream>
#include <string>

namespace octopus {

DIASPORA_REGISTER_DRIVER(octopus, octopus, OctopusDriver);

using namespace std::string_literals;

static std::string extractNamespace(const diaspora::Metadata& options) {
    auto& config = options.json();
    if(config.is_object() && config.contains("namespace") && config["namespace"].is_string())
        return config["namespace"].get<std::string>();
    return {};
}

OctopusDriver::OctopusDriver(const diaspora::Metadata& options)
: m_options(options)
, m_namespace(extractNamespace(options))
, m_disable_info_topic(extractDisableInfoTopic(options))
, m_info_topic_prefix(extractInfoTopicPrefix(options))
, m_admin(options.json().contains("octopus")
    ? static_cast<std::unique_ptr<Admin>>(std::make_unique<OctopusAdmin>(options.json(), m_namespace, m_info_topic_prefix))
    : static_cast<std::unique_ptr<Admin>>(std::make_unique<LibRdKafkaAdmin>(options.json(), m_namespace, m_info_topic_prefix)))
{}

OctopusDriver::~OctopusDriver() = default;

void OctopusDriver::createTopic(std::string_view name,
                                const diaspora::Metadata& options,
                                std::shared_ptr<diaspora::ValidatorInterface> validator,
                                std::shared_ptr<diaspora::PartitionSelectorInterface> selector,
                                std::shared_ptr<diaspora::SerializerInterface> serializer) {
    if(!options.json().is_object())
        throw diaspora::Exception{"Invalid config passed to OctopusDriver::createTopic: should be an object"};

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

    // Check if info topic creation should be disabled
    bool disable_info_topic = m_disable_info_topic;
    if(options.json().contains("disable_info_topic")
    && options.json()["disable_info_topic"].is_boolean())
        disable_info_topic = options.json()["disable_info_topic"].get<bool>();
    if(disable_info_topic) {
        auto is_default = [](const auto& component) {
            const auto& j = component->metadata().json();
            return !j.is_object() || !j.contains("type") || j["type"] == "default";
        };
        if(!is_default(validator))
            throw diaspora::Exception{
                "Cannot disable info topic: validator type is not \"default\""};
        if(!is_default(selector))
            throw diaspora::Exception{
                "Cannot disable info topic: partition selector type is not \"default\""};
        if(!is_default(serializer))
            throw diaspora::Exception{
                "Cannot disable info topic: serializer type is not \"default\""};
    }

    // Build topic specs
    std::vector<Admin::TopicSpec> topic_specs;
    topic_specs.push_back({std::string{name}, num_partitions, replication_factor});
    if(!disable_info_topic) {
        topic_specs.push_back({m_info_topic_prefix + std::string{name}, 1, replication_factor});
    }

    m_admin->createTopics(topic_specs);

    if(!disable_info_topic) {
        auto info_topic_name = m_info_topic_prefix + std::string{name};

        std::vector<std::string> messages;
        messages.push_back(validator->metadata().json().dump());
        messages.push_back(selector->metadata().json().dump());
        messages.push_back(serializer->metadata().json().dump());

        m_admin->produceMessages(info_topic_name, messages);
    }
}

std::shared_ptr<diaspora::TopicHandleInterface> OctopusDriver::openTopic(
        std::string_view name) const {
    // Check if the topic exists
    if(!m_admin->topicExists(std::string{name}))
        throw diaspora::Exception{std::string{"Topic \""} + std::string{name} + "\" not found"};

    auto kafka_name = kafkaTopicName(name);
    auto info_topic_name = m_info_topic_prefix + std::string{name};

    // Get info from topic
    diaspora::Validator validator;
    diaspora::PartitionSelector selector;
    diaspora::Serializer serializer;
    try {
        auto info_vector = m_admin->readFullTopic(info_topic_name);
        if(info_vector.size() < 3)
            throw diaspora::Exception{"Information about topic not complete in " + kafkaTopicName(info_topic_name)};
        validator = diaspora::Validator::FromMetadata(info_vector[0]);
        selector = diaspora::PartitionSelector::FromMetadata(info_vector[1]);
        serializer = diaspora::Serializer::FromMetadata(info_vector[2]);
    } catch(const diaspora::Exception&) {
        std::cerr << "[octopus:warning] Could not read info topic " << kafkaTopicName(info_topic_name)
                  << ", using default validator, serializer, and partition selector" << std::endl;
    }

    // Get the number of partitions
    size_t num_partitions = m_admin->getPartitionCount(std::string{name});

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
        std::move(kafka_name),
        validator,
        selector,
        serializer,
        targets,
        const_cast<OctopusDriver*>(this)->shared_from_this());
}

bool OctopusDriver::topicExists(std::string_view name) const {
    return m_admin->topicExists(std::string{name});
}

std::unordered_map<std::string, diaspora::Metadata> OctopusDriver::listTopics() const {
    auto all_topics = m_admin->listAllTopics();

    std::unordered_map<std::string, diaspora::Metadata> result;

    for (auto& topic_info : all_topics) {
        auto info_topic_name = m_info_topic_prefix + topic_info.name;

        try {
            auto info_vector = m_admin->readFullTopic(info_topic_name);
            if (info_vector.size() >= 3) {
                nlohmann::json metadata_json;
                metadata_json["validator"] = nlohmann::json::parse(info_vector[0]);
                metadata_json["selector"] = nlohmann::json::parse(info_vector[1]);
                metadata_json["serializer"] = nlohmann::json::parse(info_vector[2]);
                metadata_json["num_partitions"] = topic_info.partition_count;

                result[topic_info.name] = diaspora::Metadata{metadata_json};
            }
        } catch (...) {
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
    if(config.contains("namespace") && !config["namespace"].is_string())
        throw diaspora::Exception{
            "\"namespace\" option should be a string"};
    if(config.contains("disable_info_topic") && !config["disable_info_topic"].is_boolean())
        throw diaspora::Exception{
            "\"disable_info_topic\" option should be a boolean"};
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
