#include "octopus/Consumer.hpp"
#include "octopus/Driver.hpp"
#include "octopus/TopicHandle.hpp"
#include "octopus/ThreadPool.hpp"
#include "octopus/KafkaConf.hpp"
#include <librdkafka/rdkafka.h>

#include <condition_variable>

namespace octopus {

OctopusConsumer::OctopusConsumer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        std::shared_ptr<OctopusThreadPool> thread_pool,
        std::shared_ptr<OctopusTopicHandle> topic,
        diaspora::DataAllocator data_allocator,
        diaspora::DataSelector data_selector,
        const std::vector<size_t>& targets,
        std::shared_ptr<rd_kafka_t> rk)
: m_name{std::move(name)}
, m_batch_size(batch_size)
, m_max_num_batches(max_num_batches)
, m_thread_pool(std::move(thread_pool))
, m_topic(std::move(topic))
, m_data_allocator{std::move(data_allocator)}
, m_data_selector{std::move(data_selector)}
, m_target_partitions{targets}
, m_rk{std::move(rk)}
{
}

OctopusConsumer::~OctopusConsumer() {
    unsubscribe();
}

std::shared_ptr<diaspora::TopicHandleInterface> OctopusConsumer::topic() const {
      return m_topic;
}

void OctopusConsumer::unsubscribe() {
    rd_kafka_unsubscribe(m_rk.get());
}

void OctopusConsumer::process(
        diaspora::EventProcessor processor,
        std::shared_ptr<diaspora::ThreadPoolInterface> threadPool,
        diaspora::NumEvents maxEvents) {
    if(!threadPool) threadPool = m_topic->driver()->defaultThreadPool();
    size_t                  pending_events = 0;
    std::mutex              pending_mutex;
    std::condition_variable pending_cv;
    try {
        for(size_t i = 0; i < maxEvents.value; ++i) {
            auto event = pull().wait();
            {
                std::unique_lock lock{pending_mutex};
                pending_events += 1;
            }
            threadPool->pushWork([&, event=std::move(event)]() {
                    processor(event);
                    std::unique_lock lock{pending_mutex};
                    pending_events -= 1;
                    if(pending_events == 0)
                    pending_cv.notify_all();
                    });
        }
    } catch(const diaspora::StopEventProcessor&) {}
    std::unique_lock lock{pending_mutex};
    while(pending_events) pending_cv.wait(lock);
}

diaspora::Future<diaspora::Event> OctopusConsumer::pull() {
    // TODO
    throw diaspora::Exception{"OctopusConsumer::pull not implemented"};
}

}
