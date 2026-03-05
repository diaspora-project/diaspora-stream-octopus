#include "OctopusAdmin.hpp"
#include <diaspora/Exception.hpp>
#include <curl/curl.h>
#include <algorithm>
#include <cstdlib>

namespace octopus {

using namespace std::string_literals;

static const std::string DEFAULT_URL =
    "https://diaspora-web-service.qpp943wkvr7b2.us-east-1.cs.amazonlightsail.com";

static size_t writeCallback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* response = static_cast<std::string*>(userdata);
    response->append(ptr, size * nmemb);
    return size * nmemb;
}

OctopusAdmin::OctopusAdmin(const nlohmann::json& config, std::string ns)
: m_namespace(std::move(ns))
, m_kafka_admin(config, m_namespace)
{
    if(!config.contains("octopus") || !config["octopus"].is_object())
        throw diaspora::Exception{"OctopusAdmin requires an \"octopus\" object in the configuration"};

    auto& octopus_config = config["octopus"];

    if(octopus_config.contains("subject_env") && octopus_config["subject_env"].is_string()) {
        auto env_var = octopus_config["subject_env"].get<std::string>();
        auto* val = std::getenv(env_var.c_str());
        if(!val)
            throw diaspora::Exception{"Environment variable \"" + env_var + "\" (from subject_env) is not set"};
        m_subject = val;
    } else if(octopus_config.contains("subject") && octopus_config["subject"].is_string()) {
        m_subject = octopus_config["subject"].get<std::string>();
    } else {
        throw diaspora::Exception{"OctopusAdmin requires a \"subject\" or \"subject_env\" field in the \"octopus\" configuration"};
    }

    if(octopus_config.contains("authorization_env") && octopus_config["authorization_env"].is_string()) {
        auto env_var = octopus_config["authorization_env"].get<std::string>();
        auto* val = std::getenv(env_var.c_str());
        if(!val)
            throw diaspora::Exception{"Environment variable \"" + env_var + "\" (from authorization_env) is not set"};
        m_authorization = val;
    } else if(octopus_config.contains("authorization") && octopus_config["authorization"].is_string()) {
        m_authorization = octopus_config["authorization"].get<std::string>();
    } else {
        throw diaspora::Exception{"OctopusAdmin requires an \"authorization\" or \"authorization_env\" field in the \"octopus\" configuration"};
    }

    if(octopus_config.contains("url") && octopus_config["url"].is_string())
        m_url = octopus_config["url"].get<std::string>();
    else
        m_url = DEFAULT_URL;
}

std::string OctopusAdmin::performRequest(const std::string& method, const std::string& path) const {
    auto* curl = curl_easy_init();
    if(!curl)
        throw diaspora::Exception{"Failed to initialize CURL handle"};

    std::string response_body;
    std::string url = m_url + path;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.c_str());

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, ("subject: " + m_subject).c_str());
    headers = curl_slist_append(headers, ("authorization: " + m_authorization).c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    CURLcode res = curl_easy_perform(curl);

    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if(res != CURLE_OK)
        throw diaspora::Exception{
            "CURL request failed: "s + curl_easy_strerror(res)};

    if(http_code < 200 || http_code >= 300)
        throw diaspora::Exception{
            "HTTP request to " + url + " failed with status " + std::to_string(http_code)
            + ": " + response_body};

    return response_body;
}

std::vector<std::string> OctopusAdmin::fetchTopicsForNamespace() const {
    auto response = performRequest("GET", "/api/v3/namespace");

    auto json = nlohmann::json::parse(response);
    if(!json.contains("namespaces") || !json["namespaces"].is_object())
        throw diaspora::Exception{"Unexpected response format from namespace API"};

    auto& namespaces = json["namespaces"];
    if(!namespaces.contains(m_namespace))
        return {};

    auto& topics_json = namespaces[m_namespace];
    if(!topics_json.is_array())
        return {};

    std::vector<std::string> topics;
    for(auto& t : topics_json) {
        if(!t.is_string()) continue;
        auto name = t.get<std::string>();
        // Filter out __info_ topics
        if(name.rfind("__info_", 0) == 0) continue;
        topics.push_back(std::move(name));
    }
    return topics;
}

void OctopusAdmin::createTopics(const std::vector<TopicSpec>& topics) const {
    for(auto& spec : topics) {
        performRequest("POST", "/api/v3/" + m_namespace + "/" + spec.name);
    }
}

bool OctopusAdmin::topicExists(std::string_view name) const {
    auto topics = fetchTopicsForNamespace();
    return std::find(topics.begin(), topics.end(), std::string{name}) != topics.end();
}

size_t OctopusAdmin::getPartitionCount(std::string_view name) const {
    return m_kafka_admin.getPartitionCount(name);
}

std::vector<TopicInfo> OctopusAdmin::listAllTopics() const {
    auto topics = fetchTopicsForNamespace();
    std::vector<TopicInfo> result;
    result.reserve(topics.size());
    for(auto& name : topics) {
        result.push_back({std::move(name), 0});
    }
    return result;
}

void OctopusAdmin::produceMessages(std::string_view topic,
                                   const std::vector<std::string>& messages) const {
    m_kafka_admin.produceMessages(topic, messages);
}

std::vector<std::string> OctopusAdmin::readFullTopic(std::string_view name) const {
    return m_kafka_admin.readFullTopic(name);
}

}
