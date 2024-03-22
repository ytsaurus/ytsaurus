#include "client.h"

#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/http/client.h>

#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>

#include <library/cpp/json/json_reader.h>

namespace NYT::NHuggingface {

using namespace NYT::NHttp;

////////////////////////////////////////////////////////////////////////////////

THuggingfaceClient::THuggingfaceClient(const std::optional<TString>& token)
    : HuggingfaceToken_(token)
{
    auto httpsConfig = NYT::New<NYT::NHttps::TClientConfig>();
    auto poller = NConcurrency::CreateThreadPoolPoller(1, "http_poller");
    httpsConfig->MaxRedirectCount = MaxRedirectCounts_;

    Client_ = NHttps::CreateClient(httpsConfig, poller);
}

std::vector<TString> THuggingfaceClient::GetParquetFileUrls(const TString& dataset, const TString& config, const TString& split)
{
    auto headers = New<THeaders>();
    if (HuggingfaceToken_) {
        headers->Set("Authorization", "Bearer " + *HuggingfaceToken_);
    }

    TString url = NYT::Format("https://huggingface.co/api/datasets/%v/parquet/%v/%v", dataset, config, split);

    auto response = NConcurrency::WaitFor(Client_->Get(url, headers)).ValueOrThrow();

    if (response->GetStatusCode() != EStatusCode::OK) {
        THROW_ERROR_EXCEPTION("Failed to get parquet files list, HTTP proxy discovery request returned an error")
            << TErrorAttribute("status_code", response->GetStatusCode());
    }

    auto data = response->ReadAll();
    return ParseParquetFileUrls(data.ToStringBuf());
}

NConcurrency::IAsyncZeroCopyInputStreamPtr THuggingfaceClient::DownloadFile(const TString& url)
{
    auto headers = New<THeaders>();
    if (HuggingfaceToken_) {
        headers->Set("Authorization", "Bearer " + *HuggingfaceToken_);
    }

    auto response = NConcurrency::WaitFor(Client_->Get(url, headers)).ValueOrThrow();

    if (response->GetStatusCode() != EStatusCode::OK) {
        THROW_ERROR_EXCEPTION("Failed to download file, HTTP proxy discovery request returned an error")
            << TErrorAttribute("status_code", response->GetStatusCode());
    }

    return response;
}

std::vector<TString> THuggingfaceClient::ParseParquetFileUrls(TStringBuf data)
{
    NJson::TJsonValue jsonValue;
    if (!NJson::ReadJsonTree(data, &jsonValue) || !jsonValue.IsArray()) {
        THROW_ERROR_EXCEPTION("Invalid HTTP response, can't parse http body to json list");
    }
    std::vector<TString> result;
    for (const auto& fileUrl : jsonValue.GetArray()) {
        if (!fileUrl.IsString()) {
            THROW_ERROR_EXCEPTION("Invalid HTTP response, expected string element in json list");
        }
        result.push_back(fileUrl.GetString());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHuggingface
