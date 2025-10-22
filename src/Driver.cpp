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

    // TODO use libcurl and the Kafka REST API to create a topic with the given name, number of partitions and replication_factor. The m_options field of the OctopusDriver should contain a "bootstrap.servers" key with the list of Kafka servers.

    // TODO the function will also create a topic named "_<name>_info_" (with 1 partition and a replication factor identical to the above) and store the validator, selector, and serializer. These can be stored by converting them into a Metadata object and dump this metadata into a string from its JSON format (e.g. serializer.metadata().json().dump()).
}

bool OctopusDriver::topicExists(std::string_view name) const {
    // TODO check that the topic with the given name exists, using the Kafka REST API and libcurl.
}

std::shared_ptr<diaspora::TopicHandleInterface> OctopusDriver::openTopic(
        std::string_view name) const {
    // TODO open the topic with the given name. This is done by opening the "_<name>_info_" topic usig librdkafka and reading its content to get the validator, selector, and serializer back.
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
