#pragma once

#include "public.h"

#include "api.h"

#include <yt/core/http/public.h>

#include <yt/core/ytree/public.h>

#include <yt/core/rpc/authenticator.h>

#include <yt/ytlib/driver/driver.h>

#include <yt/ytlib/auth/public.h>

namespace NYT {
namespace NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

TNullable<TString> CommandNameV2ToV3(TString commandName);
TNullable<TString> CommandNameV3ToV2(TString commandName);

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

    TInstant StartTime_ = TInstant::Now();

    TNullable<TSemaphoreGuard> SemaphoreGuard_;

    NDriver::TDriverRequest DriverRequest_;
    TNullable<NDriver::TCommandDescriptor> Descriptor_;

    TNullable<int> ApiVersion_;
    TNullable<NAuth::TAuthenticationResult> Auth_;
    TNullable<NFormats::TFormat> HeadersFormat_;
    TNullable<NFormats::TFormat> InputFormat_;
    TNullable<EContentEncoding> InputCompression_;

    TNullable<NFormats::TFormat> OutputFormat_;
    TNullable<TString> ContentType_;
    TNullable<EContentEncoding> OutputCompression_;
    TNullable<TString> OutputContentEncoding_;

    TNullable<ui64> IfNoneMatch_;

    TSharedRefOutputStreamPtr MemoryOutput_;

    TError Error_;

    std::unique_ptr<NYson::IBuildingYsonConsumer<NYTree::INodePtr>> OutputParametersConsumer_;
    NYTree::IMapNodePtr OutputParameters_;

    bool OmitTrailers_ = false;

    template <class TJsonProducer>
    void DispatchJson(const TJsonProducer& producer);
    void DispatchUnauthorized(const TString& scope, const TString& message);
    void DispatchLater(const TString& retryAfter, const TString& message);
    void DispatchNotFound(const TString& message);

    void FakeError(const TString& message);

    void OnOutputParameters();
};

DEFINE_REFCOUNTED_TYPE(TContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
