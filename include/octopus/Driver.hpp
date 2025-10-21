#ifndef OCTOPUS_DRIVER_HPP
#define OCTOPUS_DRIVER_HPP

#include <diaspora/Driver.hpp>
#include <octopus/ThreadPool.hpp>
#include <octopus/TopicHandle.hpp>

namespace octopus {

class OctopusDriver : public diaspora::DriverInterface,
                     public std::enable_shared_from_this<OctopusDriver> {

    std::shared_ptr<diaspora::ThreadPoolInterface> m_default_thread_pool =
        std::make_shared<OctopusThreadPool>(diaspora::ThreadCount{0});
    std::unordered_map<std::string, std::shared_ptr<OctopusTopicHandle>> m_topics;

    public:

    void createTopic(std::string_view name,
                     const diaspora::Metadata& options,
                     std::shared_ptr<diaspora::ValidatorInterface> validator,
                     std::shared_ptr<diaspora::PartitionSelectorInterface> selector,
                     std::shared_ptr<diaspora::SerializerInterface> serializer) override {
        (void)options;
        if(m_topics.count(std::string{name})) throw diaspora::Exception{"Topic already exists"};
        std::vector<diaspora::PartitionInfo> pinfo{diaspora::PartitionInfo{"{}"}};
        if(selector) selector->setPartitions(pinfo);
        m_topics.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(std::string{name}),
            std::forward_as_tuple(
                std::make_shared<OctopusTopicHandle>(
                    std::string{name},
                    std::move(validator),
                    std::move(selector),
                    std::move(serializer),
                    shared_from_this()
                )
            )
        );
    }

    std::shared_ptr<diaspora::TopicHandleInterface> openTopic(std::string_view name) const override {
        auto it = m_topics.find(std::string{name});
        if(it == m_topics.end())
            throw diaspora::Exception{"Could not find topic \"" + std::string{name} + "\""};
        return it->second;
    }

    bool topicExists(std::string_view name) const override {
        return m_topics.count(std::string{name});
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> defaultThreadPool() const override {
        return m_default_thread_pool;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> makeThreadPool(diaspora::ThreadCount count) const override {
        return std::make_shared<OctopusThreadPool>(count);
    }

    static inline std::shared_ptr<diaspora::DriverInterface> create(const diaspora::Metadata&) {
        return std::make_shared<OctopusDriver>();
    }
};

}

#endif
