#ifndef OCTOPUS_PRODUCER_HPP
#define OCTOPUS_PRODUCER_HPP

#include <octopus/ThreadPool.hpp>
#include <octopus/TopicHandle.hpp>
#include <diaspora/Producer.hpp>
#include <librdkafka/rdkafka.h>
#include <memory>

namespace octopus {

class OctopusProducer final : public diaspora::ProducerInterface,
                              public std::enable_shared_from_this<OctopusProducer> {

    friend class OctopusTopicHandle;

    const std::string                         m_name;
    const diaspora::BatchSize                 m_batch_size;
    const diaspora::MaxNumBatches             m_max_num_batches;
    const diaspora::Ordering                  m_ordering;
    const std::shared_ptr<OctopusThreadPool>  m_thread_pool;
    const std::shared_ptr<OctopusTopicHandle> m_topic;
    const std::shared_ptr<rd_kafka_t>         m_rk;

    static void MessageDeliveryCallback(
        rd_kafka_t *rk,
        const rd_kafka_message_t *rkmessage,
        void *opaque);

    public:

    OctopusProducer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        diaspora::Ordering ordering,
        std::shared_ptr<OctopusThreadPool> thread_pool,
        std::shared_ptr<OctopusTopicHandle> topic,
        std::shared_ptr<rd_kafka_t> rk);

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

    diaspora::Future<std::optional<diaspora::EventID>> push(
            diaspora::Metadata metadata,
            diaspora::DataView data,
            std::optional<size_t> partition) override;

    diaspora::Future<std::optional<diaspora::Flushed>> flush() override;
};

}

#endif
