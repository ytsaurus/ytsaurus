#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>

#include <deque>
#include <string>
#include <utility>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

//! Input message of the "urls" stream: a URL to download and the host it belongs to.
struct TUrlMessage
    : public TYsonMessage
{
    std::string Host;
    std::string Url;

    REGISTER_YSON_STRUCT(TUrlMessage);

    static void Register(TRegistrar registrar);
};

//! Output message of the "processed_urls" stream: a URL together with its download result.
struct TProcessedUrlMessage
    : public TYsonMessage
{
    std::string Host;
    std::string Url;
    std::string Data;

    REGISTER_YSON_STRUCT(TProcessedUrlMessage);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! In-process asynchronous downloader emulating slow, throttled URL processing. One executor
//! per host drains that host's queue in the background and stores the results until the timer
//! extracts them. A plain TRefCounted persisting across epochs as a function member.
class TUrlDownloader
    : public NYT::TRefCounted
{
public:
    TUrlDownloader() = default;

    void RegisterHost(const std::string& host, const std::deque<std::string>& urls);

    void UnregisterHost(const std::string& host);

    void RegisterUrl(const std::string& host, const std::string& url);

    std::deque<std::pair<std::string, std::string>> ExtractProcessedUrls(const std::string& host);

    std::string ProcessUrl(const std::string& url) const;

private:
    THashMap<std::string, TFuture<void>> Executors_;
    THashMap<std::string, std::deque<std::string>> Queue_;
    THashMap<std::string, std::deque<std::pair<std::string, std::string>>> ProcessedUrls_;
};

using TUrlDownloaderPtr = NYT::TIntrusivePtr<TUrlDownloader>;

////////////////////////////////////////////////////////////////////////////////

//! Per-host state: the host name and the URLs still awaiting download.
struct TLimitedHostState
    : public NYTree::TYsonStruct
{
    std::string Host;
    std::deque<std::string> Urls;

    REGISTER_YSON_STRUCT(TLimitedHostState);

    static void Register(TRegistrar registrar);
};

using TLimitedHostStatePtr = NYT::TIntrusivePtr<TLimitedHostState>;

////////////////////////////////////////////////////////////////////////////////

//! Dynamic parameters of TLimitedUrlDownloadFunction, read at runtime from the dynamic
//! ``processing_function_parameters`` block via context->GetDynamicParameters<T>().
struct TDynamicLimitedUrlDownloadParameters
    : public NYTree::TYsonStruct
{
    TDuration CheckHostPeriod;

    //! Only the last PersistLimit URLs for each host are persisted. Older URLs may be lost
    //! during rebalancing. Otherwise, the host state may become too big to be stored in one row
    //! of a dynamic table.
    i64 PersistLimit = 0;

    REGISTER_YSON_STRUCT(TDynamicLimitedUrlDownloadParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Transform function limiting and downloading URLs per host. On each message it appends the URL
//! to the host state, hands it to the async downloader and schedules a check timer. On the timer
//! it emits the downloaded results, drops persisted URLs above the limit, and clears the state
//! once the host is drained.
class TLimitedUrlDownloadFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

    void ProcessTimer(
        const TInputTimerConstPtr& timer,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TMutableStateKeyClient<TLimitedHostState> HostStateClient_;
    TUrlDownloaderPtr UrlDownloader_;

    TSystemTimestamp GetNextHostCheck(const std::string& host, const IRuntimeContextPtr& context) const;

    void EnforceLimit(const TStateAccessor<TLimitedHostState>& state, const IRuntimeContextPtr& context) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
