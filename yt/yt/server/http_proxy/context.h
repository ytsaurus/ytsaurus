#pragma once

#include "private.h"
#include "http_authenticator.h"
#include "api.h"

#include <yt/yt/server/lib/misc/format_manager.h>

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/client/driver/driver.h>
#include <yt/yt/client/driver/helpers.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/rpc/authenticator.h>

#include <yt/yt/core/profiling/timing.h>

#include <util/system/thread.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TContext
    : public TRefCounted
{
public:
    TContext(
        TApiPtr api,
        NHttp::IRequestPtr request,
        NHttp::IResponseWriterPtr response);

    bool TryParseRequest();
    bool TryParseCommandName();
    bool TryParseUser();
    bool TryInitFormatManager();
    bool TryGetDescriptor();
    bool TryCheckAvailability();
    bool TryRedirectHeavyRequests();
    bool TryGetHeaderFormat();
    bool TryGetInputFormat();
    bool TryGetInputCompression();
    bool TryGetOutputFormat();
    bool TryGetOutputCompression();
    bool TryAcquireConcurrencySemaphore();

    void CaptureParameters();

    void SetContentDispositionAndMimeType();
    void SetETagRevision();
    void SetupInputStream();
    void SetupOutputStream();
    void SetupOutputParameters();
    void LogRequest();
    void LogStructuredRequest();
    void SetupTracing();
    void AddHeaders();

    void SetEnrichedError(const TError& error);

    bool TryPrepare();
    void FinishPrepare();
    void Run();
    void LogAndProfile();
    void Finalize();

private:
    const TApiPtr Api_;
    const NHttp::IRequestPtr Request_;
    const NHttp::IResponseWriterPtr Response_;

    NLogging::TLogger Logger;

    NProfiling::TWallTimer Timer_;
    TDuration WallTime_;
    TDuration CpuTime_;

    TString Parameters_;

    std::optional<TSemaphoreGuard> SemaphoreGuard_;

    NDriver::TDriverRequest DriverRequest_;
    std::optional<NDriver::TCommandDescriptor> Descriptor_;

    std::optional<int> ApiVersion_;
    std::optional<TAuthenticationResultAndToken> Auth_;
    std::unique_ptr<TFormatManager> FormatManager_;
    std::optional<NFormats::TFormat> HeadersFormat_;
    std::optional<NFormats::TFormat> InputFormat_;
    std::optional<TContentEncoding> InputContentEncoding_;

    std::optional<NFormats::TFormat> OutputFormat_;
    std::optional<TString> ContentType_;
    std::optional<TContentEncoding> OutputContentEncoding_;

    std::optional<NDriver::TEtag> IfNoneMatch_;

    bool PrepareFinished_ = false;

    TSharedRefOutputStreamPtr MemoryOutput_;

    TError Error_;

    std::unique_ptr<NYson::IBuildingYsonConsumer<NYTree::INodePtr>> OutputParametersConsumer_;
    NYTree::IMapNodePtr OutputParameters_;

    bool OmitTrailers_ = false;

    bool IsFramingEnabled_ = false;
    TFramingAsyncOutputStreamPtr FramingStream_;
    NConcurrency::TPeriodicExecutorPtr SendKeepAliveExecutor_;

    template <class TJsonProducer>
    void DispatchJson(const TJsonProducer& producer);
    void DispatchUnauthorized(const TString& scope, const TString& message);
    void DispatchUnavailable(const TError& error);
    void DispatchNotFound(const TString& message);

    void ReplyError(const TError& error);

    void OnOutputParameters();

    void ProcessFormatsInOperationSpec();
    void ProcessFormatsInParameters();

    TFramingConfigPtr GetFramingConfig() const;

    void ProcessDelayBeforeCommandTestingOption();
    void AllocateTestData(NTracing::TTraceContextPtr traceContext);
};

DEFINE_REFCOUNTED_TYPE(TContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
