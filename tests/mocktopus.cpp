#include <httplib.h>
#include <nlohmann/json.hpp>
#include <librdkafka/rdkafka.h>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <future>
#include <getopt.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

using json = nlohmann::json;

// ---------------------------------------------------------------------------
// Admin result handling (mirrors LibRdKafkaAdmin pattern)
// ---------------------------------------------------------------------------

struct AdminResult {
    rd_kafka_event_type_t event_type = RD_KAFKA_EVENT_NONE;
    struct TopicResult {
        rd_kafka_resp_err_t err;
        std::string error_string;
    };
    std::vector<TopicResult> topic_results;
};

static void adminBackgroundCb(rd_kafka_t* /*rk*/, rd_kafka_event_t* rkev, void* /*opaque*/) {
    auto* promise = static_cast<std::promise<AdminResult>*>(rd_kafka_event_opaque(rkev));
    if (!promise) return;

    AdminResult result;
    result.event_type = rd_kafka_event_type(rkev);

    switch (result.event_type) {
        case RD_KAFKA_EVENT_CREATETOPICS_RESULT: {
            size_t cnt;
            auto topics = rd_kafka_CreateTopics_result_topics(
                rd_kafka_event_CreateTopics_result(rkev), &cnt);
            for (size_t i = 0; i < cnt; i++) {
                result.topic_results.push_back({
                    rd_kafka_topic_result_error(topics[i]),
                    rd_kafka_err2str(rd_kafka_topic_result_error(topics[i]))});
            }
            break;
        }
        case RD_KAFKA_EVENT_DELETETOPICS_RESULT: {
            size_t cnt;
            auto topics = rd_kafka_DeleteTopics_result_topics(
                rd_kafka_event_DeleteTopics_result(rkev), &cnt);
            for (size_t i = 0; i < cnt; i++) {
                result.topic_results.push_back({
                    rd_kafka_topic_result_error(topics[i]),
                    rd_kafka_err2str(rd_kafka_topic_result_error(topics[i]))});
            }
            break;
        }
        default:
            break;
    }

    promise->set_value(std::move(result));
}

// ---------------------------------------------------------------------------
// Kafka helpers
// ---------------------------------------------------------------------------

static std::shared_ptr<rd_kafka_t> makeAdminClient(const std::string& bootstrap_servers) {
    char errstr[512];
    auto* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers.c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set_background_event_cb(conf, adminBackgroundCb);

    auto* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string{"Failed to create Kafka handle: "} + errstr);
    }
    return {rk, rd_kafka_destroy};
}

struct TopicSpec {
    std::string name;
    int num_partitions;
    int replication_factor;
};

static void createKafkaTopics(const std::string& bootstrap_servers,
                              const std::vector<TopicSpec>& specs) {
    auto rk = makeAdminClient(bootstrap_servers);

    std::vector<rd_kafka_NewTopic_t*> new_topics;
    std::vector<std::shared_ptr<rd_kafka_NewTopic_s>> guards;
    char errstr[512];

    for (auto& spec : specs) {
        auto* nt = rd_kafka_NewTopic_new(
            spec.name.c_str(), spec.num_partitions, spec.replication_factor,
            errstr, sizeof(errstr));
        if (!nt)
            throw std::runtime_error(std::string{"Failed to create NewTopic: "} + errstr);
        guards.emplace_back(nt, rd_kafka_NewTopic_destroy);
        new_topics.push_back(nt);
    }

    std::promise<AdminResult> promise;
    auto future = promise.get_future();

    auto* opts = rd_kafka_AdminOptions_new(rk.get(), RD_KAFKA_ADMIN_OP_CREATETOPICS);
    if (!opts) throw std::runtime_error("Failed to create AdminOptions");
    auto _opts = std::shared_ptr<rd_kafka_AdminOptions_s>{opts, rd_kafka_AdminOptions_destroy};
    rd_kafka_AdminOptions_set_opaque(opts, &promise);

    auto* rkqu = rd_kafka_queue_get_background(rk.get());
    rd_kafka_CreateTopics(rk.get(), new_topics.data(), new_topics.size(), opts, rkqu);
    rd_kafka_queue_destroy(rkqu);

    if (future.wait_for(std::chrono::seconds(60)) == std::future_status::timeout)
        throw std::runtime_error("Timed out waiting for CreateTopics result");

    auto result = future.get();
    if (result.event_type != RD_KAFKA_EVENT_CREATETOPICS_RESULT)
        throw std::runtime_error("Unexpected event type from CreateTopics");

    for (auto& tr : result.topic_results) {
        if (tr.err != RD_KAFKA_RESP_ERR_NO_ERROR)
            throw std::runtime_error("Failed to create topic: " + tr.error_string);
    }
}

static void deleteKafkaTopics(const std::string& bootstrap_servers,
                              const std::vector<std::string>& names) {
    auto rk = makeAdminClient(bootstrap_servers);

    std::vector<rd_kafka_DeleteTopic_t*> del_topics;
    std::vector<std::shared_ptr<rd_kafka_DeleteTopic_s>> guards;

    for (auto& name : names) {
        auto* dt = rd_kafka_DeleteTopic_new(name.c_str());
        if (!dt)
            throw std::runtime_error("Failed to create DeleteTopic for: " + name);
        guards.emplace_back(dt, rd_kafka_DeleteTopic_destroy);
        del_topics.push_back(dt);
    }

    std::promise<AdminResult> promise;
    auto future = promise.get_future();

    auto* opts = rd_kafka_AdminOptions_new(rk.get(), RD_KAFKA_ADMIN_OP_DELETETOPICS);
    if (!opts) throw std::runtime_error("Failed to create AdminOptions");
    auto _opts = std::shared_ptr<rd_kafka_AdminOptions_s>{opts, rd_kafka_AdminOptions_destroy};
    rd_kafka_AdminOptions_set_opaque(opts, &promise);

    auto* rkqu = rd_kafka_queue_get_background(rk.get());
    rd_kafka_DeleteTopics(rk.get(), del_topics.data(), del_topics.size(), opts, rkqu);
    rd_kafka_queue_destroy(rkqu);

    if (future.wait_for(std::chrono::seconds(60)) == std::future_status::timeout)
        throw std::runtime_error("Timed out waiting for DeleteTopics result");

    auto result = future.get();
    if (result.event_type != RD_KAFKA_EVENT_DELETETOPICS_RESULT)
        throw std::runtime_error("Unexpected event type from DeleteTopics");

    for (auto& tr : result.topic_results) {
        if (tr.err != RD_KAFKA_RESP_ERR_NO_ERROR
            && tr.err != RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART) {
            throw std::runtime_error("Failed to delete topic: " + tr.error_string);
        }
    }
}

static std::vector<std::string> listAllKafkaTopics(const std::string& bootstrap_servers) {
    char errstr[512];
    auto* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers.c_str(), errstr, sizeof(errstr));

    auto* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string{"Failed to create Kafka handle: "} + errstr);
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    const rd_kafka_metadata_t* metadata;
    rd_kafka_resp_err_t err = rd_kafka_metadata(rk, 1, nullptr, &metadata, 5000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw std::runtime_error(std::string{"Failed to get metadata: "} + rd_kafka_err2str(err));

    auto _metadata = std::shared_ptr<const rd_kafka_metadata_t>{
        metadata, rd_kafka_metadata_destroy};

    std::vector<std::string> topics;
    for (int i = 0; i < metadata->topic_cnt; i++) {
        topics.emplace_back(metadata->topics[i].topic);
    }
    return topics;
}

// ---------------------------------------------------------------------------
// Namespace helper
// ---------------------------------------------------------------------------

static std::vector<std::string> listTopicsInNamespace(const std::string& bootstrap_servers,
                                                      const std::string& ns) {
    auto all_topics = listAllKafkaTopics(bootstrap_servers);
    std::string ns_prefix = ns + ".";
    std::string info_prefix = ns + ".__info_";
    std::vector<std::string> result;
    for (auto& kafka_topic : all_topics) {
        if (kafka_topic.rfind(ns_prefix, 0) != 0) continue;
        if (kafka_topic.rfind(info_prefix, 0) == 0) continue;
        result.push_back(kafka_topic.substr(ns_prefix.size()));
    }
    return result;
}

// ---------------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------------

static httplib::Server svr;

static void signalHandler(int /*sig*/) {
    svr.stop();
}

static void printUsage(const char* prog) {
    fprintf(stderr, "Usage: %s [--port PORT] [--kafka HOST:PORT]\n", prog);
}

int main(int argc, char* argv[]) {
    int port = 8080;
    std::string kafka_bootstrap = "localhost:9092";

    static struct option long_options[] = {
        {"port",  required_argument, nullptr, 'p'},
        {"kafka", required_argument, nullptr, 'k'},
        {"help",  no_argument,       nullptr, 'h'},
        {nullptr, 0,                 nullptr,  0 }
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "p:k:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 'p': port = std::stoi(optarg); break;
            case 'k': kafka_bootstrap = optarg; break;
            case 'h':
            default:
                printUsage(argv[0]);
                return opt == 'h' ? 0 : 1;
        }
    }

    // GET /api/v3/namespace — list all topics grouped by namespace
    svr.Get("/api/v3/namespace", [&kafka_bootstrap](const httplib::Request& /*req*/,
                                                     httplib::Response& res) {
        try {
            auto all_topics = listAllKafkaTopics(kafka_bootstrap);
            std::unordered_map<std::string, std::vector<std::string>> namespaces;

            for (auto& kafka_topic : all_topics) {
                // Skip Kafka internal topics
                if (kafka_topic.rfind("__", 0) == 0) continue;

                auto dot_pos = kafka_topic.find('.');
                if (dot_pos == std::string::npos) continue;

                auto ns = kafka_topic.substr(0, dot_pos);
                auto topic_name = kafka_topic.substr(dot_pos + 1);

                // Skip __info_ topics
                if (topic_name.rfind("__info_", 0) == 0) continue;

                namespaces[ns].push_back(topic_name);
            }

            json ns_json = json::object();
            for (auto& [ns, topics] : namespaces) {
                ns_json[ns] = topics;
            }

            std::string msg = "Found " + std::to_string(namespaces.size()) + " namespaces";
            json response = {{"status", "success"}, {"message", msg}, {"namespaces", ns_json}};
            res.set_content(response.dump(), "application/json");
        } catch (const std::exception& e) {
            json err = {{"status", "error"}, {"message", e.what()}};
            res.status = 500;
            res.set_content(err.dump(), "application/json");
        }
    });

    // POST /api/v3/:namespace/:topic — create topic
    svr.Post("/api/v3/:namespace/:topic", [&kafka_bootstrap](const httplib::Request& req,
                                                              httplib::Response& res) {
        try {
            auto ns = req.path_params.at("namespace");
            auto topic = req.path_params.at("topic");

            std::string kafka_topic = ns + "." + topic;

            createKafkaTopics(kafka_bootstrap, {
                {kafka_topic, 1, 1}
            });

            auto ns_topics = listTopicsInNamespace(kafka_bootstrap, ns);
            std::string msg = "Topic " + topic + " created in " + ns;
            json response = {{"status", "success"}, {"message", msg}, {"topics", ns_topics}};
            res.set_content(response.dump(), "application/json");
        } catch (const std::exception& e) {
            json err = {{"status", "error"}, {"message", e.what()}};
            res.status = 500;
            res.set_content(err.dump(), "application/json");
        }
    });

    // DELETE /api/v3/:namespace/:topic — delete topic
    svr.Delete("/api/v3/:namespace/:topic", [&kafka_bootstrap](const httplib::Request& req,
                                                                httplib::Response& res) {
        try {
            auto ns = req.path_params.at("namespace");
            auto topic = req.path_params.at("topic");

            std::string kafka_topic = ns + "." + topic;

            deleteKafkaTopics(kafka_bootstrap, {kafka_topic});

            auto ns_topics = listTopicsInNamespace(kafka_bootstrap, ns);
            std::string msg = "Topic " + topic + " deleted from " + ns;
            json response = {{"status", "success"}, {"message", msg}, {"topics", ns_topics}};
            res.set_content(response.dump(), "application/json");
        } catch (const std::exception& e) {
            json err = {{"status", "error"}, {"message", e.what()}};
            res.status = 500;
            res.set_content(err.dump(), "application/json");
        }
    });

    // PUT /api/v3/:namespace/:topic/recreate — delete and recreate topic
    svr.Put("/api/v3/:namespace/:topic/recreate", [&kafka_bootstrap](const httplib::Request& req,
                                                                      httplib::Response& res) {
        try {
            auto ns = req.path_params.at("namespace");
            auto topic = req.path_params.at("topic");

            std::string kafka_topic = ns + "." + topic;

            deleteKafkaTopics(kafka_bootstrap, {kafka_topic});
            createKafkaTopics(kafka_bootstrap, {
                {kafka_topic, 1, 1}
            });

            json response = {{"status", "success"}, {"message", "Topic recreated"}};
            res.set_content(response.dump(), "application/json");
        } catch (const std::exception& e) {
            json err = {{"status", "error"}, {"message", e.what()}};
            res.status = 500;
            res.set_content(err.dump(), "application/json");
        }
    });

    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    int bound_port;
    if (port == 0) {
        bound_port = svr.bind_to_any_port("0.0.0.0");
        if (bound_port < 0) {
            fprintf(stderr, "mocktopus: failed to bind to any port\n");
            return 1;
        }
    } else {
        if (!svr.bind_to_port("0.0.0.0", port)) {
            fprintf(stderr, "mocktopus: failed to bind to port %d\n", port);
            return 1;
        }
        bound_port = port;
    }

    printf("mocktopus listening on port %d\n", bound_port);
    fflush(stdout);

    svr.listen_after_bind();
    return 0;
}
