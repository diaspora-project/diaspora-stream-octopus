#include "FutureState.hpp"
#include "octopus/Producer.hpp"
#include "octopus/TopicHandle.hpp"
#include <diaspora/BufferWrapperArchive.hpp>

namespace octopus {

std::shared_ptr<diaspora::TopicHandleInterface> OctopusProducer::topic() const {
    return m_topic;
}

void OctopusProducer::flush() {

}

diaspora::Future<diaspora::EventID> OctopusProducer::push(
        diaspora::Metadata metadata,
        diaspora::DataView data,
        std::optional<size_t> partition) {
    auto state = std::make_shared<FutureState<diaspora::EventID>>();
    m_thread_pool->pushWork(
            [topic=m_topic, state,
             metadata=std::move(metadata),
             data=std::move(data),
             partition]() {
            try {
            // validation
            topic->validator().validate(metadata, data);
            // serialization
            std::vector<char> metadata_buffer, data_buffer;
            diaspora::BufferWrapperOutputArchive archive(metadata_buffer);
            topic->serializer().serialize(archive, metadata);
            data_buffer.resize(data.size());
            data.read(data_buffer.data(), data_buffer.size());
            // partition selection
            auto index = topic->m_partition_selector.selectPartitionFor(metadata, partition);
            if(index != 0) throw diaspora::Exception{"Invalid index returned by PartitionSelector"};
            auto& metadata_vector = topic->m_partition.metadata;
            auto& data_vector = topic->m_partition.data;
            metadata_vector.push_back(std::move(metadata_buffer));
            data_vector.push_back(std::move(data_buffer));
            auto event_id = metadata_vector.size()-1;
            // set the ID
            state->set(event_id);
            } catch(const diaspora::Exception& ex) {
                state->set(ex);
            }
        });
    return diaspora::Future<diaspora::EventID>{
        [state] { return state->wait(); },
            [state] { return state->test(); }
    };
}

}
