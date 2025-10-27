#include "FutureState.hpp"
#include "octopus/Producer.hpp"
#include "octopus/TopicHandle.hpp"
#include <diaspora/BufferWrapperArchive.hpp>
#include <exception>

namespace octopus {

struct Message {
    std::vector<char>                               payload;
    std::shared_ptr<FutureState<diaspora::EventID>> future = std::make_shared<FutureState<diaspora::EventID>>();
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

void OctopusProducer::flush() {
    while (true) {
        int remaining = rd_kafka_outq_len(m_rk.get());
        if (remaining == 0)
            break;
        rd_kafka_flush(m_rk.get(), 1000);
    }
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

    msg->future->set(static_cast<diaspora::EventID>(rkmessage->offset));
    delete msg;
}

diaspora::Future<diaspora::EventID> OctopusProducer::push(
        diaspora::Metadata metadata,
        diaspora::DataView data,
        std::optional<size_t> partition) {
    auto msg = new Message{};
    auto state = msg->future;
    m_thread_pool->pushWork(
            [this, topic=m_topic, msg,
             metadata=std::move(metadata),
             data=std::move(data),
             partition]() {
            auto state = msg->future;
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
                state->set(ex);
            }
        });
    auto p = shared_from_this();
    return diaspora::Future<diaspora::EventID>{
        [state, producer=p] {
            while(!state->test()) {
                rd_kafka_poll(producer->m_rk.get(), 100);
            }
            return state->wait();
        },
        [state] { return state->test(); }
    };
}

}
