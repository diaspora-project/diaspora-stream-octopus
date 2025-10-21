#ifndef DDD_PRODUCER_HPP
#define DDD_PRODUCER_HPP

#include <AAA/ThreadPool.hpp>
#include <AAA/TopicHandle.hpp>
#include <diaspora/Producer.hpp>

namespace BBB {

class CCCProducer final : public diaspora::ProducerInterface {

    const std::string                     m_name;
    const diaspora::BatchSize             m_batch_size;
    const diaspora::MaxNumBatches         m_max_num_batches;
    const diaspora::Ordering              m_ordering;
    const std::shared_ptr<CCCThreadPool>  m_thread_pool;
    const std::shared_ptr<CCCTopicHandle> m_topic;

    public:

    CCCProducer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        diaspora::Ordering ordering,
        std::shared_ptr<CCCThreadPool> thread_pool,
        std::shared_ptr<CCCTopicHandle> topic)
    : m_name{std::move(name)}
    , m_batch_size(batch_size)
    , m_max_num_batches(max_num_batches)
    , m_ordering(ordering)
    , m_thread_pool(std::move(thread_pool))
    , m_topic(std::move(topic)) {}

    const std::string& name() const override {
        return m_name;
    }

    diaspora::BatchSize batchSize() const override {
        return m_batch_size;
    }

    diaspora::MaxNumBatches maxNumBatches() const override {
        return m_max_num_batches;
    }

    diaspora::Ordering ordering() const override {
        return m_ordering;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> threadPool() const override {
        return m_thread_pool;
    }

    std::shared_ptr<diaspora::TopicHandleInterface> topic() const override;

    diaspora::Future<diaspora::EventID> push(
            diaspora::Metadata metadata,
            diaspora::DataView data,
            std::optional<size_t> partition) override;

    void flush() override;
};

}

#endif
