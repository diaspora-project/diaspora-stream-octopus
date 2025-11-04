#include "AwsMskIamSigner.hpp"

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/DateTime.h>

#include <openssl/evp.h>
#include <openssl/buffer.h>
#include <cstring>

static std::string base64UrlEncode(const std::string &input)
{
    BIO *b64 = BIO_new(BIO_f_base64());
    BIO *mem = BIO_new(BIO_s_mem());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL); // No newlines
    b64 = BIO_push(b64, mem);

    BIO_write(b64, input.data(), static_cast<int>(input.size()));
    BIO_flush(b64);

    BUF_MEM *mem_ptr;
    BIO_get_mem_ptr(b64, &mem_ptr);

    std::string output(mem_ptr->data, mem_ptr->length);
    BIO_free_all(b64);

    // URL-safe transformation
    for (char &c : output)
    {
        if (c == '+')
            c = '-';
        else if (c == '/')
            c = '_';
    }

    // Remove padding '='
    output.erase(std::remove(output.begin(), output.end(), '='), output.end());

    return output;
}

AwsMskIamSigner::AwsMskIamSigner(const std::string &region, int64_t expiration_seconds)
    : url("https://kafka." + region + ".amazonaws.com/"), expiration_seconds(expiration_seconds)
{

    Aws::Client::ClientConfiguration config;
    config.region = region;

    credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();
    signer = std::make_shared<Aws::Client::AWSAuthV4Signer>(
        credentials_provider,
        "kafka-cluster",
        region);
}

AwsMskIamSigner::Token AwsMskIamSigner::generateToken() const
{
    Aws::Http::URI uri(url);
    uri.AddQueryStringParameter("Action", "kafka-cluster:Connect");

    Aws::Http::Standard::StandardHttpRequest request(uri, Aws::Http::HttpMethod::HTTP_GET);
    if (!signer->PresignRequest(request, expiration_seconds))
    {
        throw std::runtime_error("Failed to presign MSK IAM request");
    }

    const std::string presigned_url = request.GetURIString();
    // has to be in the end of the constructed url; AddQueryStringParameter does not work
    std::string full_url = presigned_url + "&User-Agent=octopus-signer-cpp%2F0.0.1";
    Aws::Utils::DateTime now = Aws::Utils::DateTime::Now();
    int64_t expiration = now.Millis() + expiration_seconds * 1000;

    std::string encoded = base64UrlEncode(full_url);
    // std::cout << "Encoded token: " << encoded << " (full URL: " << full_url << ")" << std::endl;
    return {encoded, expiration};
}
