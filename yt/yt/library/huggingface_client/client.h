#pragma once

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/http.h>

namespace NYT::NHuggingface {

////////////////////////////////////////////////////////////////////////////////

class THuggingfaceClient
{
public:
    THuggingfaceClient(
        const std::optional<TString>& token,
        NConcurrency::IPollerPtr poller,
        const std::optional<TString>& urlOverride = std::nullopt); // For tests only.

    std::vector<TString> GetParquetFileUrls(const TString& dataset, const TString& config, const TString& split);

    NConcurrency::IAsyncZeroCopyInputStreamPtr DownloadFile(const TString& url);

private:
    static constexpr int MaxRedirectCount = 10;

    const TString Url_;
    const std::optional<TString> Token_;
    const NHttp::IClientPtr Client_;

    std::vector<TString> ParseParquetFileUrls(TStringBuf data);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHuggingface
