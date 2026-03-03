#ifndef OCTOPUS_DRIVER_HPP
#define OCTOPUS_DRIVER_HPP

#include <diaspora/Driver.hpp>
#include <diaspora/PosixThreadPool.hpp>
#include <octopus/TopicHandle.hpp>

namespace octopus {

class OctopusTopicHandle;

class OctopusDriver : public diaspora::DriverInterface,
                      public std::enable_shared_from_this<OctopusDriver> {

    friend class OctopusTopicHandle;

    std::shared_ptr<diaspora::ThreadPoolInterface> m_default_thread_pool =
        std::make_shared<diaspora::PosixThreadPool>(diaspora::ThreadCount{0});
    const diaspora::Metadata m_options;
    const std::string m_namespace;
    const bool m_disable_info_topic;

    static std::string extractNamespace(const diaspora::Metadata& options) {
        auto& config = options.json();
        if(config.is_object() && config.contains("namespace") && config["namespace"].is_string())
            return config["namespace"].get<std::string>();
        return {};
    }

    static bool extractDisableInfoTopic(const diaspora::Metadata& options) {
        auto& config = options.json();
        if(config.is_object() && config.contains("disable_info_topic")
        && config["disable_info_topic"].is_boolean())
            return config["disable_info_topic"].get<bool>();
        return false;
    }

    std::string kafkaTopicName(std::string_view name) const {
        if(m_namespace.empty()) return std::string{name};
        return m_namespace + "." + std::string{name};
    }

    public:

    OctopusDriver(const diaspora::Metadata& options)
    : m_options(options)
    , m_namespace(extractNamespace(options))
    , m_disable_info_topic(extractDisableInfoTopic(options)) {}

    void createTopic(std::string_view name,
                     const diaspora::Metadata& options,
                     std::shared_ptr<diaspora::ValidatorInterface> validator,
                     std::shared_ptr<diaspora::PartitionSelectorInterface> selector,
                     std::shared_ptr<diaspora::SerializerInterface> serializer) override;

    std::shared_ptr<diaspora::TopicHandleInterface> openTopic(std::string_view name) const override;

    bool topicExists(std::string_view name) const override;

    std::unordered_map<std::string, diaspora::Metadata> listTopics() const override;

    std::shared_ptr<diaspora::ThreadPoolInterface> defaultThreadPool() const override;

    std::shared_ptr<diaspora::ThreadPoolInterface> makeThreadPool(diaspora::ThreadCount count) const override;

    static std::shared_ptr<diaspora::DriverInterface> create(const diaspora::Metadata& options);
};

}

#endif
