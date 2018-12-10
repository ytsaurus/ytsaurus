#pragma once

#include "public.h"

#include "api.h"

#include <yt/core/http/public.h>

#include <yt/core/ytree/public.h>

#include <yt/core/rpc/authenticator.h>

#include <yt/ytlib/driver/driver.h>

#include <yt/ytlib/auth/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

std::optional<TString> CommandNameV2ToV3(TString commandName);
std::optional<TString> CommandNameV3ToV2(TString commandName);

////////////////////////////////////////////////////////////////////////////////

class TContext
    : public TRefCounted
{
public:
    TContext(
        const TApiPtr& api,
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp);

    void ProcessDebugHeaders();

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
    void AddHeaders();

    void SetError(const TError& error);

    bool TryPrepare();
    void FinishPrepare();
    void Run();
    void Finalize();

private:
    const TApiPtr Api_;
    const NHttp::IRequestPtr Request_;
    const NHttp::IResponseWriterPtr Response_;

    NLogging::TLogger Logger;

    TInstant StartTime_ = TInstant::Now();

    std::optional<TSemaphoreGuard> SemaphoreGuard_;

    NDriver::TDriverRequest DriverRequest_;
    std::optional<NDriver::TCommandDescriptor> Descriptor_;

    std::optional<int> ApiVersion_;
    std::optional<NAuth::TAuthenticationResult> Auth_;
    std::optional<NFormats::TFormat> HeadersFormat_;
    std::optional<NFormats::TFormat> InputFormat_;
    std::optional<TContentEncoding> InputContentEncoding_;

    std::optional<NFormats::TFormat> OutputFormat_;
    std::optional<TString> ContentType_;
    std::optional<TContentEncoding> OutputContentEncoding_;

    std::optional<ui64> IfNoneMatch_;

    TSharedRefOutputStreamPtr MemoryOutput_;

    TError Error_;

    std::unique_ptr<NYson::IBuildingYsonConsumer<NYTree::INodePtr>> OutputParametersConsumer_;
    NYTree::IMapNodePtr OutputParameters_;

    bool OmitTrailers_ = false;

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
