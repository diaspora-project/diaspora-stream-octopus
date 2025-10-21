#include "octopus/TopicHandle.hpp"
#include "octopus/Driver.hpp"
#include "octopus/Producer.hpp"
#include "octopus/Consumer.hpp"

namespace octopus {

std::shared_ptr<diaspora::DriverInterface> OctopusTopicHandle::driver() const {
    return m_driver;
}

std::shared_ptr<diaspora::ProducerInterface>
OctopusTopicHandle::makeProducer(std::string_view name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_batch,
        diaspora::Ordering ordering,
        std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
        diaspora::Metadata options) {
    (void)options;
    if(!thread_pool) thread_pool = m_driver->makeThreadPool(diaspora::ThreadCount{0});
    auto simple_thread_pool = std::dynamic_pointer_cast<OctopusThreadPool>(thread_pool);
    if(!simple_thread_pool)
        throw diaspora::Exception{"ThreadPool should be an instance of OctopusThreadPool"};
    return std::make_shared<OctopusProducer>(
            std::string{name}, batch_size, max_batch, ordering, simple_thread_pool,
            shared_from_this());
}

std::shared_ptr<diaspora::ConsumerInterface>
OctopusTopicHandle::makeConsumer(std::string_view name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_batch,
        std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
        diaspora::DataAllocator data_allocator,
        diaspora::DataSelector data_selector,
        const std::vector<size_t>& targets,
        diaspora::Metadata options) {
    (void)options;
    (void)targets;
    if(!thread_pool) thread_pool = m_driver->makeThreadPool(diaspora::ThreadCount{0});
    auto simple_thread_pool = std::dynamic_pointer_cast<OctopusThreadPool>(thread_pool);
    if(!simple_thread_pool)
        throw diaspora::Exception{"ThreadPool should be an instance of OctopusThreadPool"};
    return std::make_shared<OctopusConsumer>(
            std::string{name}, batch_size, max_batch, simple_thread_pool,
            shared_from_this(), std::move(data_allocator),
            std::move(data_selector));
}

}
