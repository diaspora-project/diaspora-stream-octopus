#include "octopus/TopicHandle.hpp"
#include "octopus/Driver.hpp"
#include "octopus/Producer.hpp"
#include "octopus/Consumer.hpp"
#include "octopus/KafkaConf.hpp"

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
    auto pool = std::dynamic_pointer_cast<OctopusThreadPool>(thread_pool);
    if(!pool)
        throw diaspora::Exception{"ThreadPool should be an instance of OctopusThreadPool"};

    // Create the configuration for the producer
    KafkaConf kconf{m_driver->m_options};
    kconf.add(options);
    rd_kafka_conf_set_dr_msg_cb(kconf, OctopusProducer::MessageDeliveryCallback);
    if(batch_size.value > 0)
        kconf["batch.num.messages"] = std::to_string(std::min<size_t>(1000000ULL, batch_size.value));

    // Create a producer instance
    char errstr[512];
    auto conf = kconf.dup(); // rd_kafka_new will take ownership if successful
    auto rk = std::shared_ptr<rd_kafka_t>{
        rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)),
        rd_kafka_destroy};
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{"Could not create rd_kafka_t instance: " + std::string{errstr}};
    }

    return std::make_shared<OctopusProducer>(
            std::string{name}, batch_size, max_batch, ordering, pool,
            shared_from_this(), std::move(rk));
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
    auto pool = std::dynamic_pointer_cast<OctopusThreadPool>(thread_pool);
    if(!pool)
        throw diaspora::Exception{"ThreadPool should be an instance of OctopusThreadPool"};
    return std::make_shared<OctopusConsumer>(
            std::string{name}, batch_size, max_batch, pool,
            shared_from_this(), std::move(data_allocator),
            std::move(data_selector));
}

}
