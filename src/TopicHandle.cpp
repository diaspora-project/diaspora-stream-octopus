#include "octopus/TopicHandle.hpp"
#include "octopus/Driver.hpp"
#include "octopus/Producer.hpp"
#include "octopus/Consumer.hpp"
#include "octopus/KafkaConf.hpp"
#include "KafkaHelper.hpp"

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
    if(!thread_pool) thread_pool = m_driver->makeThreadPool(diaspora::ThreadCount{0});
    auto pool = std::dynamic_pointer_cast<OctopusThreadPool>(thread_pool);
    if(!pool)
        throw diaspora::Exception{"ThreadPool should be an instance of OctopusThreadPool"};

    // Create the configuration for the producer
    KafkaConf kconf{m_driver->m_options.json()["kafka"]};
    auto& opt = options.json();
    if(opt.is_object() && opt.contains("kafka") && opt["kafka"].is_object())
        kconf.add(opt["kafka"]);
    rd_kafka_conf_set_dr_msg_cb(kconf, OctopusProducer::MessageDeliveryCallback);
    kconf["enable.idempotence"] = "true";
    if(batch_size.value > 0)
        kconf["batch.num.messages"] = std::to_string(std::min<size_t>(1000000ULL, batch_size.value));

    // Create a producer instance
    char errstr[512];
    auto conf = kconf.dup(); // rd_kafka_new will take ownership if successful
    applyAwsAuthIfConfigured(conf, m_driver->m_options.json()["kafka"]);
    auto rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{"Could not create rd_kafka_t instance: " + std::string{errstr}};
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    return std::make_shared<OctopusProducer>(
            std::string{name}, batch_size, max_batch, ordering, pool,
            shared_from_this(), std::move(_rk));
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
    if(!thread_pool) thread_pool = m_driver->makeThreadPool(diaspora::ThreadCount{0});
    auto pool = std::dynamic_pointer_cast<OctopusThreadPool>(thread_pool);
    if(!pool)
        throw diaspora::Exception{"ThreadPool should be an instance of OctopusThreadPool"};

    // Create list of partitions
    std::shared_ptr<rd_kafka_topic_partition_list_s> topic_list;
    if(targets.empty()) {
        topic_list = std::shared_ptr<rd_kafka_topic_partition_list_s>{
                rd_kafka_topic_partition_list_new(1),
                rd_kafka_topic_partition_list_destroy};
        rd_kafka_topic_partition_list_add(
                topic_list.get(), m_name.c_str(), RD_KAFKA_PARTITION_UA);
    } else {
        topic_list = std::shared_ptr<rd_kafka_topic_partition_list_s>{
                rd_kafka_topic_partition_list_new(targets.size()),
                rd_kafka_topic_partition_list_destroy};
        for(auto t : targets) {
            rd_kafka_topic_partition_list_add(
                    topic_list.get(), m_name.c_str(),
                    static_cast<int32_t>(t));
        }
    }

    // Create the configuration for the producer
    KafkaConf kconf{m_driver->m_options.json()["kafka"]};
    kconf["enable.partition.eof"] = "false";
    kconf["auto.offset.reset"] = "earliest";
    kconf["topic.metadata.refresh.interval.ms"] = "10000";
    auto& opt = options.json();
    if(opt.is_object() && opt.contains("kafka") && opt["kafka"].is_object())
        kconf.add(opt["kafka"]);
    if(batch_size.value > 0) {
        std::cerr << "[octopus:warning] BatchSize ignored by consumer "
                  << "(use a BatchSize of 0 to remove this message)" << std::endl;
    }
    if(max_batch.value > 0) {
        std::cerr << "[octopus:warning] MaxNumBatches ignored by consumer "
                  << "(use a MaxNumBatches of 0 to remove this message)" << std::endl;
    }
    kconf["group.id"] = name;

    // Create a consumer instance
    char errstr[512];
    auto conf = kconf.dup(); // rd_kafka_new will take ownership if successful
    applyAwsAuthIfConfigured(conf, m_driver->m_options.json()["kafka"]);
    auto rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw diaspora::Exception{
            "Could not create rd_kafka_t instance: " + std::string{errstr}};
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    // Subscribe
    if (rd_kafka_subscribe(rk, topic_list.get()) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw diaspora::Exception{
            std::string{"Failed to subscribe to topic: "}
            + rd_kafka_err2str(rd_kafka_last_error())};
    }

    return std::make_shared<OctopusConsumer>(
            std::string{name}, batch_size, max_batch, pool,
            shared_from_this(), std::move(data_allocator),
            std::move(data_selector), targets, _rk);
}

}
