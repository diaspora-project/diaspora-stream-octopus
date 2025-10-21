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
    const diaspora::Metadata m_options;

    public:

    OctopusDriver(const diaspora::Metadata& options)
    : m_options(options) {}

    void createTopic(std::string_view name,
                     const diaspora::Metadata& options,
                     std::shared_ptr<diaspora::ValidatorInterface> validator,
                     std::shared_ptr<diaspora::PartitionSelectorInterface> selector,
                     std::shared_ptr<diaspora::SerializerInterface> serializer) override;

    std::shared_ptr<diaspora::TopicHandleInterface> openTopic(std::string_view name) const override;

    bool topicExists(std::string_view name) const override;

    std::shared_ptr<diaspora::ThreadPoolInterface> defaultThreadPool() const override;

    std::shared_ptr<diaspora::ThreadPoolInterface> makeThreadPool(diaspora::ThreadCount count) const override;

    static std::shared_ptr<diaspora::DriverInterface> create(const diaspora::Metadata& options);
};

}

#endif
