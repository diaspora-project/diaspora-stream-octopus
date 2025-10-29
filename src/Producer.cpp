#include "FutureState.hpp"
#include "octopus/Producer.hpp"
#include "octopus/TopicHandle.hpp"
#include <diaspora/BufferWrapperArchive.hpp>
#include <exception>

namespace octopus {

struct Message {
 
    using ResultType = std::variant<std::monostate, diaspora::EventID, diaspora::Exception>;

    std::vector<char>           payload;
    std::shared_ptr<ResultType> result = std::make_shared<ResultType>();
};

OctopusProducer::OctopusProducer(
    std::string name,
    diaspora::BatchSize batch_size,
    diaspora::MaxNumBatches max_num_batches,
    diaspora::Ordering ordering,
    std::shared_ptr<OctopusThreadPool> thread_pool,
    std::shared_ptr<OctopusTopicHandle> topic,
    std::shared_ptr<rd_kafka_t> rk)
    : m_name{std::move(name)}
    , m_batch_size(batch_size)
    , m_max_num_batches(max_num_batches)
    , m_ordering(ordering)
    , m_thread_pool(std::move(thread_pool))
    , m_topic(std::move(topic))
    , m_rk{std::move(rk)} {}

std::shared_ptr<diaspora::TopicHandleInterface> OctopusProducer::topic() const {
    return m_topic;
}

diaspora::Future<std::optional<diaspora::Flushed>> OctopusProducer::flush() {
    return {
        [rk=m_rk](int timeout_ms) -> std::optional<diaspora::Flushed> {
            rd_kafka_flush(rk.get(), timeout_ms);
            if(rd_kafka_outq_len(rk.get()) == 0) return diaspora::Flushed{};
            else return std::nullopt;
        },
        [rk=m_rk]() { return rd_kafka_outq_len(rk.get()) == 0; }
    };
}

void OctopusProducer::MessageDeliveryCallback(
    rd_kafka_t *rk,
    const rd_kafka_message_t *rkmessage,
    void *opaque) {

    (void)rk;
    (void)opaque;
    Message* msg = static_cast<Message*>(rkmessage->_private);
    if(!msg) {
        std::cerr << "Unexpected null rkmessage->_private" << std::endl;
        return;
    }

    *(msg->result) = static_cast<diaspora::EventID>(rkmessage->offset);
    delete msg;
}

diaspora::Future<std::optional<diaspora::EventID>> OctopusProducer::push(
        diaspora::Metadata metadata,
        diaspora::DataView data,
        std::optional<size_t> partition) {
    auto msg = new Message{};
    auto result_ptr = msg->result;
    m_thread_pool->pushWork(
            [this, topic=m_topic, msg,
             metadata=std::move(metadata),
             data=std::move(data),
             partition,
             result_ptr]() {
            try {
                // validation
                topic->validator().validate(metadata, data);
                // beginning of the buffer will hold sizes
                size_t initial_size = 2*sizeof(size_t);
                msg->payload.resize(initial_size);
                // wrap the buffer and serialize the metadata
                diaspora::BufferWrapperOutputArchive archive(msg->payload);
                topic->serializer().serialize(archive, metadata);
                // write the size of the metadata at the beginning
                size_t metadata_size = msg->payload.size() - 2*sizeof(size_t);
                std::memcpy(msg->payload.data(), &metadata_size, sizeof(metadata_size));
                // write the size of the data right after
                size_t data_size = data.size();
                std::memcpy(msg->payload.data() + sizeof(metadata_size), &data_size, sizeof(data_size));
                // add the data
                size_t data_offset = msg->payload.size();
                msg->payload.resize(data_offset + data.size());
                data.read(msg->payload.data() + data_offset, data.size());
                // partition selection
                auto index = topic->m_partition_selector.selectPartitionFor(metadata, partition);
                if(index != 0) throw diaspora::Exception{"Invalid index returned by PartitionSelector"};
                // send message
                rd_kafka_resp_err_t err = rd_kafka_producev(
                    m_rk.get(),
                    RD_KAFKA_V_TOPIC(m_topic->m_name.c_str()),
                    RD_KAFKA_V_VALUE(msg->payload.data(), msg->payload.size()),
                    RD_KAFKA_V_OPAQUE(msg),
                    RD_KAFKA_V_END
                );
                if (err) throw diaspora::Exception{
                    "rd_kafka_producev failed: " + std::string{rd_kafka_err2str(err)}};
                // poll once
                rd_kafka_poll(m_rk.get(), 0);
            } catch(const diaspora::Exception& ex) {
                delete msg;
                *result_ptr = ex;
            }
        });
    auto p = shared_from_this();
    return {
        [result_ptr, producer=p](int timeout_ms) -> std::optional<diaspora::EventID> {
            if(std::holds_alternative<std::monostate>(*result_ptr))
                rd_kafka_poll(producer->m_rk.get(), timeout_ms);
            if(std::holds_alternative<diaspora::EventID>(*result_ptr))
                return std::get<diaspora::EventID>(*result_ptr);
            else if(std::holds_alternative<diaspora::Exception>(*result_ptr))
                throw std::get<diaspora::Exception>(*result_ptr);
            else return std::nullopt;
        },
        [result_ptr] { return !std::holds_alternative<std::monostate>(*result_ptr); }
    };
}

}
