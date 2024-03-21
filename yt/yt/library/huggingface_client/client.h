#pragma once

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/http.h>

namespace NYT::NHuggingface {

////////////////////////////////////////////////////////////////////////////////

class THuggingfaceClient
{
public:
    explicit THuggingfaceClient(const std::optional<TString>& token);

    std::vector<TString> GetParquetFileUrls(const TString& dataset, const TString& config, const TString& split);

    NConcurrency::IAsyncZeroCopyInputStreamPtr DownloadFile(const TString& url);

private:
    static constexpr int MaxRedirectCounts_ = 10;

    std::optional<TString> HuggingfaceToken_;
    NHttp::IClientPtr Client_;

    std::vector<TString> ParseParquetFileUrls(TStringBuf data);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHuggingface
