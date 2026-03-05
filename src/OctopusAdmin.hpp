#ifndef OCTOPUS_OCTOPUS_ADMIN_HPP
#define OCTOPUS_OCTOPUS_ADMIN_HPP

#include "Admin.hpp"
#include "LibRdKafkaAdmin.hpp"
#include <nlohmann/json.hpp>
#include <string>

namespace octopus {

class OctopusAdmin : public Admin {

    std::string m_url;
    std::string m_subject;
    std::string m_authorization;
    std::string m_namespace;
    std::string m_info_topic_prefix;
    LibRdKafkaAdmin m_kafka_admin;

    std::string performRequest(const std::string& method, const std::string& path) const;
    std::vector<std::string> fetchTopicsForNamespace() const;

    public:

    OctopusAdmin(const nlohmann::json& config, std::string ns,
                 std::string info_topic_prefix = "info_");

    void createTopics(const std::vector<TopicSpec>& topics) const override;

    bool topicExists(std::string_view name) const override;

    size_t getPartitionCount(std::string_view name) const override;

    std::vector<TopicInfo> listAllTopics() const override;

    void produceMessages(std::string_view topic, const std::vector<std::string>& messages) const override;

    std::vector<std::string> readFullTopic(std::string_view name) const override;
};

}

#endif
