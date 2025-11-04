#pragma once

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/http/URI.h>
#include <aws/core/http/standard/StandardHttpRequest.h>

#include <memory>
#include <string>

class AwsMskIamSigner
{
public:
    struct Token
    {
        std::string token;
        int64_t expiration_ms;
    };

    AwsMskIamSigner(const std::string &region, int64_t expiration_seconds = 900);
    Token generateToken() const;

private:
    std::string url;
    int64_t expiration_seconds;

    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider;
    std::shared_ptr<Aws::Client::AWSAuthV4Signer> signer;
};
