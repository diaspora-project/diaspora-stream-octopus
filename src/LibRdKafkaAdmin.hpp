#ifndef OCTOPUS_LIBRDKAFKA_ADMIN_HPP
#define OCTOPUS_LIBRDKAFKA_ADMIN_HPP

#include "Admin.hpp"
#include <nlohmann/json.hpp>

namespace octopus {

class LibRdKafkaAdmin : public Admin {

    nlohmann::json m_config;
    std::string m_namespace;

    std::string kafkaTopicName(std::string_view name) const;

    public:

    explicit LibRdKafkaAdmin(const nlohmann::json& config, std::string ns);

    void createTopics(const std::vector<TopicSpec>& topics) const override;

    bool topicExists(std::string_view name) const override;

    size_t getPartitionCount(std::string_view name) const override;

    std::vector<TopicInfo> listAllTopics() const override;

    void produceMessages(std::string_view topic, const std::vector<std::string>& messages) const override;

    std::vector<std::string> readFullTopic(std::string_view name) const override;
};

}

#endif
