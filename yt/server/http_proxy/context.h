#pragma once

#include "public.h"
#include "private.h"

#include "api.h"

#include <yt/ytlib/auth/public.h>

#include <yt/client/driver/driver.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/http/public.h>

#include <yt/core/ytree/public.h>

#include <yt/core/rpc/authenticator.h>

#include <yt/server/http_proxy/http_authenticator.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TContext
    : public TRefCounted
{
public:
    TContext(
        const TApiPtr& api,
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp);

    bool TryParseRequest();
    bool TryParseCommandName();
    bool TryParseUser();
    bool TryGetDescriptor();
    bool TryCheckMethod();
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

    void SetError(const TError& error);

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

    TInstant StartTime_ = TInstant::Now();
    TDuration Duration_;

    TString Parameters_;

    std::optional<TSemaphoreGuard> SemaphoreGuard_;

    NDriver::TDriverRequest DriverRequest_;
    std::optional<NDriver::TCommandDescriptor> Descriptor_;

    std::optional<int> ApiVersion_;
    std::optional<TAuthenticationResultAndToken> Auth_;
    std::optional<NFormats::TFormat> HeadersFormat_;
    std::optional<NFormats::TFormat> InputFormat_;
    std::optional<TContentEncoding> InputContentEncoding_;

    std::optional<NFormats::TFormat> OutputFormat_;
    std::optional<TString> ContentType_;
    std::optional<TContentEncoding> OutputContentEncoding_;

    std::optional<ui64> IfNoneMatch_;

    bool PrepareFinished_ = false;

    TSharedRefOutputStreamPtr MemoryOutput_;

    TError Error_;

    std::unique_ptr<NYson::IBuildingYsonConsumer<NYTree::INodePtr>> OutputParametersConsumer_;
    NYTree::IMapNodePtr OutputParameters_;

    bool OmitTrailers_ = false;

    bool IsFramingEnabled_ = false;
    NConcurrency::TPeriodicExecutorPtr SendKeepAliveExecutor_;

    template <class TJsonProducer>
    void DispatchJson(const TJsonProducer& producer);
    void DispatchUnauthorized(const TString& scope, const TString& message);
    void DispatchUnavailable(const TString& retryAfter, const TString& message);
    void DispatchNotFound(const TString& message);

    void ReplyError(const TError& error);
    void ReplyFakeError(const TString& message);

    void OnOutputParameters();
};

DEFINE_REFCOUNTED_TYPE(TContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
