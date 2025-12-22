#include "octopus/Consumer.hpp"
#include "octopus/Driver.hpp"
#include "octopus/TopicHandle.hpp"
#include "octopus/KafkaConf.hpp"
#include "octopus/Event.hpp"

#include <diaspora/BufferWrapperArchive.hpp>
#include <librdkafka/rdkafka.h>

#include <condition_variable>

namespace octopus {

struct FutureEventState {

    std::shared_ptr<OctopusConsumer>  consumer;
    std::variant<std::monostate,
                 diaspora::Event,
                 diaspora::Exception> value;

    FutureEventState(std::shared_ptr<OctopusConsumer> c)
    : consumer{std::move(c)} {}

    void fetch(int timeout_ms) {
        if(!std::holds_alternative<std::monostate>(value)) return;
        auto rkmessage = rd_kafka_consumer_poll(consumer->m_rk.get(), timeout_ms);
        if (!rkmessage) {
            return;
        }
        auto _rkmessage = std::shared_ptr<rd_kafka_message_s>{
            rkmessage, rd_kafka_message_destroy};
        if (rkmessage->err) {
            if (rkmessage->err == RD_KAFKA_RESP_ERR__FATAL) {
                char fatal_errstr[512];
                rd_kafka_resp_err_t ferr = rd_kafka_fatal_error(
                        consumer->m_rk.get(), fatal_errstr, sizeof(fatal_errstr));
                value = diaspora::Exception{
                    std::string{"Kafka error: "} + rd_kafka_err2str(ferr)
                        +  "(" + fatal_errstr + ")"};
            } else {
                return;
            }
        }
        // No error, process the message
        const char* payload = static_cast<const char*>(rkmessage->payload);
        auto payload_len = rkmessage->len;
        if(payload_len < 2*sizeof(size_t)) {
            value = diaspora::Exception{
                "Unexpected message size received (payload < 2*sizeof(size_t))"};
            return;
        }
        // Get size of metadata and size of data
        size_t metadata_size, data_size;
        std::memcpy(&metadata_size, payload, sizeof(metadata_size));
        std::memcpy(&data_size, payload+sizeof(metadata_size), sizeof(data_size));

        // Check for an end-of-topic message
        if(metadata_size == std::numeric_limits<size_t>::max()
        && data_size == std::numeric_limits<size_t>::max()) {
            value = diaspora::Event{
                std::make_shared<OctopusEvent>(
                    diaspora::Metadata{},
                    diaspora::DataView{},
                    diaspora::PartitionInfo{nlohmann::json{{"partition_id", rkmessage->partition}}},
                    diaspora::NoMoreEvents
                )};
            return;
        }

        if(metadata_size + data_size + 2*sizeof(size_t) != payload_len) {
            value = diaspora::Exception{
                "Unexpected message size received (payload size doesn't match"
                    " declared metadata size and data size)"};
            return;
        }
        try {
            // Deserialize the metadata
            auto serializer = consumer->m_topic->serializer();
            diaspora::BufferWrapperInputArchive archive{
                std::string_view{payload + 2*sizeof(size_t), metadata_size}};
            diaspora::Metadata metadata;
            serializer.deserialize(archive, metadata);

            // Select the data
            auto& data_selector = consumer->m_data_selector;
            auto descriptor = diaspora::DataDescriptor{"", data_size};
            descriptor = data_selector ? data_selector(metadata, descriptor) : diaspora::DataDescriptor{};

            // Copy the data to its final location
            auto& data_allocator = consumer->m_data_allocator;
            diaspora::DataView data_view;
            if(data_allocator) {
                data_view = data_allocator(metadata, descriptor);
                auto data_ptr = payload + 2*sizeof(size_t) + metadata_size;
                size_t data_view_offset = 0;
                for(auto& segment : descriptor.flatten()) {
                    data_view.write(data_ptr + segment.offset, segment.size, data_view_offset);
                    data_view_offset += segment.size;
                }
            }

            // Get partition info
            diaspora::PartitionInfo partition_info{
                nlohmann::json{{"partition_id", rkmessage->partition}}};

            // Create the Event
            auto event = std::make_shared<octopus::OctopusEvent>(
                std::move(metadata), std::move(data_view),
                std::move(partition_info), rkmessage->offset);

            // Assign to the value
            value = diaspora::Event{std::move(event)};

        } catch(const diaspora::Exception& ex) {
            value = ex;
        }
    }
};

OctopusConsumer::OctopusConsumer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
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
        int timeout_ms,
        diaspora::NumEvents maxEvents,
        std::shared_ptr<diaspora::ThreadPoolInterface> threadPool) {
    if(!threadPool) threadPool = m_topic->driver()->defaultThreadPool();
    size_t                  pending_events = 0;
    std::mutex              pending_mutex;
    std::condition_variable pending_cv;
    try {
        for(size_t i = 0; i < maxEvents.value; ++i) {
            auto event = pull().wait(timeout_ms);
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

diaspora::Future<std::optional<diaspora::Event>> OctopusConsumer::pull() {

    auto state = std::make_shared<FutureEventState>(shared_from_this());

    auto wait_fn = [state](int timeout_ms) -> std::optional<diaspora::Event> {
        state->fetch(timeout_ms);
        if(std::holds_alternative<diaspora::Event>(state->value))
            return std::get<diaspora::Event>(state->value);
        else if(std::holds_alternative<diaspora::Exception>(state->value))
            throw std::get<diaspora::Exception>(state->value);
        else
            return std::nullopt;
    };

    auto test_fn = [state]() -> bool {
        state->fetch(1);
        return !std::holds_alternative<std::monostate>(state->value);
    };

    return diaspora::Future<std::optional<diaspora::Event>>{
        std::move(wait_fn), std::move(test_fn)
    };
}

}
