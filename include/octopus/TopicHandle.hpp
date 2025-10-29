#ifndef OCTOPUS_TOPIC_HANDLE_HPP
#define OCTOPUS_TOPIC_HANDLE_HPP

#include <diaspora/TopicHandle.hpp>

#include <vector>

namespace octopus {

class OctopusDriver;

class OctopusTopicHandle final : public diaspora::TopicHandleInterface,
                             public std::enable_shared_from_this<OctopusTopicHandle> {

    friend class OctopusProducer;
    friend class OctopusConsumer;

    struct Partition {
        std::vector<std::vector<char>> metadata;
        std::vector<std::vector<char>> data;
    };

    const std::string                          m_name;
    const std::vector<diaspora::PartitionInfo> m_pinfo;
    const diaspora::Validator                  m_validator;
    const diaspora::PartitionSelector          m_partition_selector;
    const diaspora::Serializer                 m_serializer;
    const std::shared_ptr<OctopusDriver>       m_driver;

    public:

    OctopusTopicHandle(
        std::string name,
        diaspora::Validator validator,
        diaspora::PartitionSelector partition_selector,
        diaspora::Serializer serializer,
        std::vector<diaspora::PartitionInfo> targets,
        std::shared_ptr<OctopusDriver> driver)
    : m_name{std::move(name)}
    , m_pinfo{std::move(targets)}
    , m_validator(std::move(validator))
    , m_partition_selector(std::move(partition_selector))
    , m_serializer(std::move(serializer))
    , m_driver{std::move(driver)}
    {}

    const std::string& name() const override {
        return m_name;
    }

    std::shared_ptr<diaspora::DriverInterface> driver() const override;

    const std::vector<diaspora::PartitionInfo>& partitions() const override {
        return m_pinfo;
    }

    diaspora::Validator validator() const override {
        return m_validator;
    }

    diaspora::PartitionSelector selector() const override {
        return m_partition_selector;
    }

    diaspora::Serializer serializer() const override {
        return m_serializer;
    }

    std::shared_ptr<diaspora::ProducerInterface>
        makeProducer(std::string_view name,
                     diaspora::BatchSize batch_size,
                     diaspora::MaxNumBatches max_batch,
                     diaspora::Ordering ordering,
                     std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
                     diaspora::Metadata options) override;

    std::shared_ptr<diaspora::ConsumerInterface>
        makeConsumer(std::string_view name,
                     diaspora::BatchSize batch_size,
                     diaspora::MaxNumBatches max_batch,
                     std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
                     diaspora::DataAllocator data_allocator,
                     diaspora::DataSelector data_selector,
                     const std::vector<size_t>& targets,
                     diaspora::Metadata options) override;
};

}

#endif
