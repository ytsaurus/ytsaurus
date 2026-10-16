#pragma once

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/http.h>

namespace NYT::NHuggingface {

////////////////////////////////////////////////////////////////////////////////

class THuggingfaceClient
{
public:
    THuggingfaceClient(
        const std::optional<std::string>& token,
        NConcurrency::IPollerPtr poller,
        const std::optional<std::string>& urlOverride = std::nullopt); // For tests only.

    std::vector<std::string> GetParquetFileUrls(const std::string& dataset, const std::string& subset, const std::string& split);

    NConcurrency::IAsyncZeroCopyInputStreamPtr DownloadFile(const std::string& url);

private:
    static constexpr int MaxRedirectCount = 10;

    const std::string Url_;
    const std::optional<std::string> Token_;
    const NHttp::IClientPtr Client_;

    std::vector<std::string> ParseParquetFileUrls(TStringBuf data);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHuggingface
