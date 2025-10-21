#ifndef DDD_CONSUMER_HPP
#define DDD_CONSUMER_HPP

#include <AAA/ThreadPool.hpp>
#include <AAA/TopicHandle.hpp>

#include <diaspora/Consumer.hpp>

namespace BBB {

class CCCConsumer final : public diaspora::ConsumerInterface {

    const std::string                     m_name;
    const diaspora::BatchSize             m_batch_size;
    const diaspora::MaxNumBatches         m_max_num_batches;
    const std::shared_ptr<CCCThreadPool>  m_thread_pool;
    const std::shared_ptr<CCCTopicHandle> m_topic;
    const diaspora::DataAllocator         m_data_allocator;
    const diaspora::DataSelector          m_data_selector;

    size_t                                m_next_offset = 0;

    public:

    CCCConsumer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        std::shared_ptr<CCCThreadPool> thread_pool,
        std::shared_ptr<CCCTopicHandle> topic,
        diaspora::DataAllocator data_allocator,
        diaspora::DataSelector data_selector);

    const std::string& name() const override {
        return m_name;
    }

    diaspora::BatchSize batchSize() const override {
        return m_batch_size;
    }

    diaspora::MaxNumBatches maxNumBatches() const override {
        return m_max_num_batches;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> threadPool() const override {
        return m_thread_pool;
    }

    std::shared_ptr<diaspora::TopicHandleInterface> topic() const override;

    const diaspora::DataAllocator& dataAllocator() const override {
        return m_data_allocator;
    }

    const diaspora::DataSelector& dataSelector() const override {
        return m_data_selector;
    }

    void process(diaspora::EventProcessor processor,
                 std::shared_ptr<diaspora::ThreadPoolInterface> threadPool,
                 diaspora::NumEvents maxEvents) override;

    void unsubscribe() override;

    diaspora::Future<diaspora::Event> pull() override;

};

}

#endif
