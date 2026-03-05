#ifndef OCTOPUS_ADMIN_HPP
#define OCTOPUS_ADMIN_HPP

#include <string>
#include <string_view>
#include <vector>

namespace octopus {

struct TopicInfo {
    std::string name;
    size_t partition_count;
};

class Admin {

    public:

    struct TopicSpec {
        std::string name;
        size_t num_partitions;
        size_t replication_factor;
    };

    virtual ~Admin() = default;

    virtual void createTopics(const std::vector<TopicSpec>& topics) const = 0;

    virtual bool topicExists(std::string_view name) const = 0;

    virtual size_t getPartitionCount(std::string_view name) const = 0;

    virtual std::vector<TopicInfo> listAllTopics() const = 0;

    virtual void produceMessages(std::string_view topic, const std::vector<std::string>& messages) const = 0;

    virtual std::vector<std::string> readFullTopic(std::string_view name) const = 0;
};

}

#endif
