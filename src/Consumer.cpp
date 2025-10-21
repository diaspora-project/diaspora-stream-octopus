#include "AAA/Consumer.hpp"
#include "AAA/Driver.hpp"
#include "AAA/TopicHandle.hpp"
#include "AAA/ThreadPool.hpp"

#include <condition_variable>

namespace BBB {

CCCConsumer::CCCConsumer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        std::shared_ptr<CCCThreadPool> thread_pool,
        std::shared_ptr<CCCTopicHandle> topic,
        diaspora::DataAllocator data_allocator,
        diaspora::DataSelector data_selector)
: m_name{std::move(name)}
, m_batch_size(batch_size)
, m_max_num_batches(max_num_batches)
, m_thread_pool(std::move(thread_pool))
, m_topic(std::move(topic))
, m_data_allocator{std::move(data_allocator)}
, m_data_selector{std::move(data_selector)}
{}

std::shared_ptr<diaspora::TopicHandleInterface> CCCConsumer::topic() const {
      return m_topic;
}

void CCCConsumer::unsubscribe() {}

void CCCConsumer::process(
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

diaspora::Future<diaspora::Event> CCCConsumer::pull() {
    // TODO
    throw diaspora::Exception{"CCCConsumer::pull not implemented"};
}

}
