#ifndef OCTOPUS_KAFKA_CONF_HPP
#define OCTOPUS_KAFKA_CONF_HPP

#include <diaspora/Exception.hpp>
#include <diaspora/Metadata.hpp>
#include <librdkafka/rdkafka.h>
#include <memory>
#include <sstream>
#include <string>

namespace octopus {

class KafkaConf {

    std::shared_ptr<rd_kafka_conf_t> m_config;

    struct PropertySetter {

        std::shared_ptr<rd_kafka_conf_t> m_config;
        std::string                      m_name;

        template<typename StringType>
        inline PropertySetter(std::shared_ptr<rd_kafka_conf_t> config, StringType&& name)
        : m_config{config}
        , m_name{std::forward<StringType>(name)} {}

        PropertySetter(const PropertySetter&) = delete;
        PropertySetter(PropertySetter&&) = delete;

        inline void operator=(const char* value) {
            char errstr[512];
            auto ret = rd_kafka_conf_set(m_config.get(), m_name.c_str(), value,
                                         errstr, sizeof(errstr));
            if (ret != RD_KAFKA_CONF_OK) {
                throw diaspora::Exception{"Could not set Kafka configuration: " + std::string(errstr)};
            }
        }

        inline void operator=(const std::string& value) {
            *this = value.c_str();
        }
    };

    inline KafkaConf(rd_kafka_conf_t* conf)
    : m_config{conf, rd_kafka_conf_destroy} {}

    public:

    inline KafkaConf()
    : m_config{rd_kafka_conf_new(), rd_kafka_conf_destroy} {}

    inline KafkaConf(const KafkaConf& other)
    : KafkaConf(rd_kafka_conf_dup(other.m_config.get())) {}

    inline KafkaConf(KafkaConf&& other) = default;

    inline KafkaConf& operator=(const KafkaConf& other) {
        if(this == &other) return *this;
        m_config = std::shared_ptr<rd_kafka_conf_t>{
            rd_kafka_conf_dup(other.m_config.get()),
            rd_kafka_conf_destroy};
        return *this;
    }

    inline KafkaConf& operator=(KafkaConf&& other) = default;

    template <typename StringType>
    inline auto operator[](StringType&& name) {
        return PropertySetter{m_config, std::forward<StringType>(name)};
    }

    auto dup() const {
        return m_config ? rd_kafka_conf_dup(m_config.get()) : nullptr;
    }

    inline operator rd_kafka_conf_t* () const {
        return m_config.get();
    }

    void add(const diaspora::Metadata& conf) {
        if(!conf.json().is_object())
            throw diaspora::Exception{"Metadata passed to KafkaConf should be an object"};
        for(auto& p : conf.json().items()) {
            if(p.value().is_string()) {
                (*this)[p.key()] = p.value().get_ref<const std::string&>();
            } else if(p.value().is_array()) {
                std::stringstream ss;
                for(auto& e : p.value()) {
                    ss << (e.is_string() ? e.get_ref<const std::string&>() : e.dump()) << ",";
                }
                auto value = ss.str();
                if(value.size() > 0 ) value.resize(value.size()-1);
                (*this)[p.key()] = value;
            } else {
                (*this)[p.key()] = p.value().dump();
            }
        }
    }

    inline KafkaConf(const diaspora::Metadata& conf)
    : KafkaConf() {
        add(conf);
    }

};

}

#endif
