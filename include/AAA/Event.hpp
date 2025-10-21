#ifndef DDD_EVENT_HPP
#define DDD_EVENT_HPP

#include <diaspora/Event.hpp>

namespace BBB {

class SimpleEvent : public diaspora::EventInterface {

    diaspora::Metadata      m_metadata;
    diaspora::DataView      m_data;
    diaspora::PartitionInfo m_partition;
    diaspora::EventID       m_id;

    public:

    SimpleEvent(diaspora::Metadata metadata,
                diaspora::DataView data,
                diaspora::PartitionInfo partition,
                diaspora::EventID id)
    : m_metadata(std::move(metadata))
    , m_data(std::move(data))
    , m_partition(std::move(partition))
    , m_id(id) {}

    diaspora::Metadata metadata() const override {
        return m_metadata;
    }

    diaspora::DataView data() const override {
        return m_data;
    }

    diaspora::PartitionInfo partition() const override {
        return m_partition;
    }

    diaspora::EventID id() const override {
        return m_id;
    }

    void acknowledge() const override {}

};

}

#endif
